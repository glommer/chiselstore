use std::io::Write;
use structopt::StructOpt;
use tokio::io::{AsyncBufReadExt, BufReader};

pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{Consistency, Query};

#[derive(StructOpt, Debug)]
#[structopt(name = "gouge")]
struct Opt {
    #[structopt(short, long, default_value = "http://127.0.0.1:50001")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let stdin = tokio::io::stdin();
    let rdr = BufReader::new(stdin);
    let mut lines = rdr.lines();
    print!("gouge=# ");
    std::io::stdout().flush().unwrap();
    while let Some(line) = lines.next_line().await? {
        let mut client = RpcClient::connect(opt.addr.clone()).await?;
        let query = tonic::Request::new(Query {
            sql: line.to_string(),
            consistency: Consistency::RelaxedReads as i32,
        });
        let response = client.execute(query).await?;
        let response = response.into_inner();
        for row in response.rows {
            println!("{:?}", row.values);
        }
        print!("gouge=# ");
        std::io::stdout().flush().unwrap();
    }
    Ok(())
}
