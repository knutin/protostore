use std::io;

use bytes::{BytesMut, Bytes, Buf, BufMut, IntoBuf};
use byteorder::BigEndian;

use tokio_io::codec::{Decoder, Encoder};


#[derive(Debug, PartialEq)]
pub enum RequestType {
    Read,
    Write,
}

#[derive(Debug)]
pub struct Request {
    pub reqtype: RequestType,
    pub id: u32,
    pub uuid: [u8; 16],
    pub body: Option<BytesMut>,
}

#[derive(Debug)]
pub struct Response {
    pub id: u32,
    pub body: Bytes,
}

pub struct Protocol {
    pub len: Option<usize>,
}



impl Decoder for Protocol {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Request>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        if self.len.is_none() {
            let len = buf.split_to(4).into_buf().get_u32::<BigEndian>();
            self.len = Some(len as usize);
        }

        if buf.len() < self.len.unwrap() {
            return Ok(None);
        }

        match buf[0] {
            b'W' => {
                let mut header = buf.split_to(1 + 16 + 4).into_buf();
                header.advance(1); // skip type

                let mut uuid: [u8; 16] = [0; 16];
                header.copy_to_slice(&mut uuid);
                let id = header.get_u32::<BigEndian>();
                let body = buf.split_to(self.len.unwrap() - (1 + 16 + 4));

                self.len = None;
                Ok(Some(Request {
                            reqtype: RequestType::Write,
                            id: id,
                            uuid: uuid,
                            body: Some(body),
                        }))

            }

            b'R' => {
                let mut buf = buf.split_to(1 + 16 + 4).into_buf();
                buf.advance(1);

                let mut uuid: [u8; 16] = [0; 16];
                buf.copy_to_slice(&mut uuid);
                let id = buf.get_u32::<BigEndian>();

                self.len = None;
                Ok(Some(Request {
                            reqtype: RequestType::Read,
                            id: id,
                            uuid: uuid,
                            body: None,
                        }))
            }

            any => {
                panic!("got unexpected request type: {}", any);
            }
        }
    }
}

impl Encoder for Protocol {
    type Item = Response;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(18 + item.body.len());
        dst.put_u32::<BigEndian>(item.id);
        dst.put_u16::<BigEndian>(item.body.len() as u16);
        dst.put_slice(&item.body);
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, BigEndian};
    use bytes::{BytesMut, Buf, BufMut, IntoBuf};

    use tokio_io::codec::{Decoder, Encoder};
    use {Protocol, Request, RequestType, Response};


    #[test]
    fn decode_write() {
        let mut buf = BytesMut::with_capacity(128);
        let uuid = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let reqid = 42;
        let data = BytesMut::from(vec![0, 2, 4, 8]);

        buf.put_u32::<BigEndian>(1 + 16 + 4 + data.len() as u32);
        buf.put(b'W');
        buf.put_slice(&uuid);
        buf.put_u32::<BigEndian>(reqid);
        buf.put_slice(&data);

        assert_eq!(4 + 1 + 16 + 4 + data.len(), buf.len());

        let mut proto = Protocol { len: None };
        let decoded = proto.decode(&mut buf);
        assert!(decoded.is_ok());
        let request = decoded.unwrap().unwrap();

        assert_eq!(RequestType::Write, request.reqtype);
        assert_eq!(reqid, request.id);
        assert_eq!(uuid, request.uuid);
        assert_eq!(Some(data), request.body);
        assert_eq!(0, buf.len());
    }

    #[test]
    fn decode_read() {
        let mut buf = BytesMut::with_capacity(128);
        let uuid = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let reqid = 42;

        buf.put_u32::<BigEndian>(1 + 16 + 4);
        buf.put(b'R');
        buf.put_slice(&uuid);
        buf.put_u32::<BigEndian>(reqid);

        let mut proto = Protocol { len: None };
        let decoded = proto.decode(&mut buf);
        assert!(decoded.is_ok());
        let request = decoded.unwrap().unwrap();

        assert_eq!(RequestType::Read, request.reqtype);
        assert_eq!(reqid, request.id);
        assert_eq!(uuid, request.uuid);
        assert_eq!(None, request.body);
    }

    #[test]
    fn encode() {
        let uuid = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let reqid = 42;
        let mut body = BytesMut::with_capacity(4);
        body.put_u32::<BigEndian>(45);

        let response = Response {
            id: reqid,
            body: body.freeze(),
        };

        let mut proto = Protocol { len: None };
        let mut encoded = BytesMut::with_capacity(128);
        let result = proto.encode(response, &mut encoded);
        assert!(result.is_ok());
        assert_eq!(10, encoded.len());


        assert_eq!(42, encoded.split_to(4).into_buf().get_u32::<BigEndian>());
        assert_eq!(4, encoded.split_to(2).into_buf().get_u16::<BigEndian>());
        assert_eq!(45, encoded.split_to(4).into_buf().get_u32::<BigEndian>());
        assert_eq!(0, encoded.len());
    }
}
