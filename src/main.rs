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
        let (socket, peer_addr) = listener.accept().await?;
        println!("{} 접속함", peer_addr);

        // 송신 핸들을 클론하고 rx에다는 송신 핸들을 구독함
        let tx = tx.clone();
        let mut rx = tx.subscribe();
        // spawn은 비동기 경량 스레드 생성이라네요? 그래서 async를 쓰는거고 async move는 async는 비동기 move는 소유권 이전
        // 정리하자면 spawn은 접속자마다 1ㄷ1로 처리할 스레드를 만드는거고, async는 내부에서 await쓰고 move는 소켓, 채널, 변수 선언했던거 전부 소유권을 이전하는거라고 하네요
        tokio::spawn(async move {
            // socket.into_split()은 반은 read 반은 write로 해주는거라네요 레이스컨디션 방지용이라고 합니다. 
            // 읽으면서 동시에 쓰면 안되니까..
            let (reader, mut writer) = socket.into_split();
            // 일단 이건 나눠준 부분중에 reader만 BufReader(버퍼 리더겠죠?)로 가져오는것같고
            let mut reader = BufReader::new(reader);
            // line은 아래보면 result 저장하는 부분이죠 이건 뭐 입출력 배웠으니까 알고요
            let mut line = String::new();

            loop{
                // !는 매크로 문법입니다. 러스트는 컴파일타임에 엄격한 통제를 하기 때문에 있다네요. rs만의 특징 신기하네요.
                tokio::select! {
                    // socket으로 받은걸 line에 저장하고 result(아마 에러통제?용으로 위에서 만든거일겁니다.)에 저장합니다.
                    result = reader.read_line(&mut line) =>{
                        // 에러처리네요 조금 넘어가겠습니다.
                        match result {
                            Ok(0) => break,
                            Ok(_) => {
                                println!("[전송 로그] {}", line.trim());
                                // 에러 안나면 다른 클라이언트에 뿌려줘라 뭐 그런 의미인것 같네요.
                                let _ = tx.send((line.clone(), peer_addr));
                                // 비워주고
                                line.clear();
                            }
                            Err(_) => break,
                        }
                    }
                    // rx.recv는 리시브인가
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
