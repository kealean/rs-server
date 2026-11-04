use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 주소 생성
    let addr = "0.0.0.0:1234";
    // TcpListener생성 후 에러체크
    let listener = TcpListener::bind(addr).await?;
    println!("서버 실행: {}", addr);

    // 링버퍼 기반 원형 큐 생성 후 최대 수용 크기를 100개로 정한다, 송/수신 헨들을 저장함 _는 보관한다는 의미
    let (tx, _rx) = broadcast::channel::<(String, SocketAddr)>(100);
    // 메인루프 진입
    loop{
        // listener가 상대가 접속했는지 비동기적으로 확인하고(os에서 tcp소켓 여기로 수신됨 확인좀 이라고 할때까지 기다림) 에러체크함
        let (soket, peer_addr) = listener.accept().await?;
        println!("{} 접속함", peer_addr);

        // 송신 핸들을 클론하고 rx에다는 송신 핸들을 구독함
        let tx = tx.clone();
        let mut rx = tx.subscribe();
        // spawn은 비동기 경량 스레드 생성이라네요? 그래서 async를 쓰는거고 async move는 async는 비동기 move는 소유권 이전
        //정리하자면 spawn은 접속자마다 1ㄷ1로 처리할 스레드를 만드는거고, async는 내부에서 await쓰고 move는 소켓, 채널, 변수 선언했던거 전부 소유권을 이전하는거라고 하네요
        tokio::spawn(async move {
            let (reader, mut writer) = soket.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop{
                tokio::select! {
                    result = reader.read_line(&mut line) =>{
                        match result {
                            Ok(0) => break,
                            Ok(_) => {
                                println!("[전송 로그] {}", line.trim());
                                let _ = tx.send((line.clone(), peer_addr));
                                line.clear();
                            }
                            Err(_) => break,
                        }
                    }
                    Ok((msg, sender_addr)) = rx.recv() => {
                        if sender_addr != peer_addr {
                            let formatted = format!("{}\n", msg.trim_end());
                            if writer.write_all(formatted.as_bytes()).await.is_err(){
                                break;
                            }
                        }
                    }
                }
            }
            println!("종료");
        });
    }
}
