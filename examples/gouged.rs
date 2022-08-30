use anyhow::{anyhow, Context, Result};
use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::fs;
use tonic::transport::Server;
use url::Url;
use yaml_rust::YamlLoader;

#[derive(StructOpt, Debug)]
#[structopt(name = "gouged")]
struct Opt {
    #[structopt(short, long, default_value = ".conf.yaml")]
    conf: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let s = fs::read_to_string(&opt.conf).await?;
    let docs = YamlLoader::load_from_str(&s).unwrap();
    let doc = &docs[0];
    let nodes = doc
        .as_vec()
        .ok_or_else(|| anyhow!("malformed yaml: not an array: {:?}", doc))?;
    let mut peers = vec![];
    let mut all_nodes: Vec<String> = vec![];
    let mut my_port = None;
    let mut my_id = 0;

    let me = Url::parse(
        &std::env::var("CHISELSTORE_ADDR")
            .with_context(|| "reading CHISELSTORE_ADDR")
            .unwrap(),
    )?;

    for (id, node) in nodes.iter().enumerate() {
        let id = id + 1;
        let rpc_str = node
            .as_str()
            .ok_or_else(|| anyhow!("{:?} is not a string", node))?;

        let rpc = Url::parse(rpc_str)?;
        if me != rpc {
            peers.push(id)
        } else {
            my_id = id;
            my_port = rpc.port().clone()
        }
        all_nodes.push(rpc.to_string());
    }

    let all_nodes = Arc::new(all_nodes);
    let node_rpc_addr = Box::new(move |id: usize| -> String {
        assert!(id >= 1);
        let peer = id - 1;
        all_nodes[peer].clone()
    });

    let rpc_port = my_port
        .ok_or_else(|| anyhow!("no port found. Am I ({:?}) on the node list?", me.as_str()))?;
    assert_ne!(my_id, 0);

    let rpc_listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port);

    let transport = RpcTransport::new(node_rpc_addr);
    let server = StoreServer::start(my_id, peers, transport)?;
    let server = Arc::new(server);
    let f = {
        let server = server.clone();
        tokio::task::spawn_blocking(move || {
            server.run();
        })
    };
    let rpc = RpcService::new(server);
    let g = tokio::task::spawn(async move {
        println!("RPC listening to {} ...", rpc_listen_addr);
        let ret = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve(rpc_listen_addr)
            .await;
        ret
    });
    let results = tokio::try_join!(f, g)?;
    results.1?;
    Ok(())
}
