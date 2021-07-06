use bytes::{BufMut, Bytes, BytesMut};
use futures::{
    task::{Context, Poll},
    Stream, StreamExt,
};
use std::pin::Pin;

pub type BytesStream<E: std::error::Error + Send + Sync + 'static> =
    Box<dyn Stream<Item = Result<Bytes, E>> + Send + Sync + Unpin>;
pub struct ChunkStream<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    buf: BytesMut,
    cap: usize,
    inner: BytesStream<E>,
}

impl<E> ChunkStream<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new(stream: BytesStream<E>) -> Self {
        Self {
            buf: BytesMut::with_capacity(8 * 1024),
            cap: 8 * 1024,
            inner: stream,
        }
    }
    pub fn with_capacity(stream: BytesStream<E>, size: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(size),
            cap: size,
            inner: stream,
        }
    }
}

impl<E> Stream for ChunkStream<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, E>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cap = self.cap;
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => match res {
                Ok(bs) => {
                    self.buf.put_slice(&bs);
                    if self.buf.len() >= cap {
                        return Poll::Ready(Some(Ok(self.buf.split_to(cap).freeze())));
                    } else {
                        return Poll::Pending;
                    }
                }
                Err(e) => {
                    return Poll::Ready(Some(Err(e.into())));
                }
            },
            Poll::Ready(None) => {
                if self.buf.len() > 0 {
                    if self.buf.len() >= cap {
                        return Poll::Ready(Some(Ok(self.buf.split_to(cap).freeze())));
                    } else {
                        return Poll::Ready(Some(Ok(self.buf.split().freeze())));
                    }
                } else {
                    return Poll::Ready(None);
                }
            }
            Poll::Pending => {
                return Poll::Pending;
            }
        }
    }
}

#[tokio::test]
async fn test_chunk_stream() {
    use futures::stream::iter;
    let mut l = Vec::new();
    for _ in 0..3 {
        let mut buf = BytesMut::new();
        for j in 0..255 {
            buf.put_u8(j as u8);
        }
        l.push(buf.freeze());
    }
    let s = iter(l).map(|v| Ok::<Bytes, std::convert::Infallible>(v));
    let mut cs = ChunkStream::with_capacity(Box::new(s), 100);
    while let Some(Ok(bs)) = cs.next().await {
        println!("{}", bs.len());
    }
}
