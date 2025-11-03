use std::collections::HashMap;
use std::net::{TcpStream, UdpSocket};
use std::io::{Read, Write};
use std::time::Duration;
use std::sync::{Mutex, Arc};

use crate::{Result, Error, WingResponse};
use crate::node::{WingNodeDef, WingNodeData};
use crate::propmap::NAME_TO_DEF;

use rustler::{NifStruct, NifTaggedEnum};


#[derive(NifTaggedEnum, Debug, Clone)]
pub enum Meter {
    Channel(u8),
    Aux(u8),
    Bus(u8),
    Main(u8),
    Matrix(u8),
    Dca(u8),
    Fx(u8),
    Source(u8),
    Output(u8),
    Monitor,
    Rta,
    Channel2(u8),
    Aux2(u8),
    Bus2(u8),
    Main2(u8),
    Matrix2(u8)
}

lazy_static::lazy_static! {
    static ref ID_TO_NAME: HashMap<i32, Vec<String>> = {
        let mut id2name = HashMap::<i32, Vec<String>>::new();
        if id2name.is_empty() {
            for (fullname, def) in NAME_TO_DEF.iter() {
                id2name.get_mut(&def.id).map(|x| x.push(fullname.to_string())).unwrap_or_else(|| {
                    id2name.insert(def.id, vec![fullname.to_string()]);
                });
            }
        }
        id2name
    };
}

const RX_BUFFER_SIZE: usize = 2048;
const DATA_KEEP_ALIVE_SECONDS: u64 = 7;
const METERS_KEEP_ALIVE_SECONDS: u64 = 3;

// Wing protocol command constants
const CMD_INT_INLINE_MAX: u8 = 0x3f;       // 0-63: Inline integer values
const CMD_NODE_INDEX_MAX: u8 = 0x7f;       // 64-127: Node index requests
const CMD_SHORT_STRING_MIN: u8 = 0x80;     // 128-191: Short strings (1-64 bytes)
const CMD_SHORT_STRING_MAX: u8 = 0xbf;
const CMD_MED_STRING_MIN: u8 = 0xc0;       // 192-207: Medium strings
const CMD_MED_STRING_MAX: u8 = 0xcf;
const CMD_EMPTY_STRING: u8 = 0xd0;         // 208: Empty string
const CMD_LONG_STRING: u8 = 0xd1;          // 209: Long string (up to 256 bytes)
const CMD_NODE_INDEX_16: u8 = 0xd2;        // 210: 16-bit node index
const CMD_INT16: u8 = 0xd3;                // 211: 16-bit integer
const CMD_INT32: u8 = 0xd4;                // 212: 32-bit integer
const CMD_FLOAT32: u8 = 0xd5;              // 213: 32-bit float
const CMD_FLOAT32_ALT: u8 = 0xd6;          // 214: 32-bit float (alternate)
const CMD_SET_NODE_ID: u8 = 0xd7;          // 215: Set current node ID
const CMD_CLICK: u8 = 0xd8;                // 216: Click action
const CMD_STEP: u8 = 0xd9;                 // 217: Step action
const CMD_GOTO_ROOT: u8 = 0xda;            // 218: Navigate to root
const CMD_GO_UP: u8 = 0xdb;                // 219: Navigate up one level
const CMD_REQUEST_DATA: u8 = 0xdc;         // 220: Request data
const CMD_REQUEST_DEF: u8 = 0xdd;          // 221: Request node definition
const CMD_REQUEST_END: u8 = 0xde;          // 222: End of request
const CMD_NODE_DEF: u8 = 0xdf;             // 223: Node definition follows
const ESCAPE_BYTE: u8 = 0xdf;              // 223: Escape byte
const ESCAPED_ESCAPE: u8 = 0xde;           // 222: Escaped escape byte

#[derive(NifStruct)]
#[module = "Wing.DiscoveryInfo"]
pub struct DiscoveryInfo {
    pub ip:       String,
    pub name:     String,
    pub model:    String,
    pub serial:   String,
    pub firmware: String,
}

#[derive(Debug)]
pub struct Meters {
    pub socket: UdpSocket,
    pub port: u16,
}

#[derive(Debug)]
struct _WingConsoleMain {
    keep_alive_timer:        std::time::Instant,
    rx_buf:                  [u8; RX_BUFFER_SIZE],
    rx_buf_tail:             usize,
    rx_buf_size:             usize,
    rx_esc:                  bool,
    rx_current_channel:      i8,
    rx_has_in_pipe:          Option<u8>,
    current_node_id:         i32,
}

#[derive(Debug)]
struct _WingConsoleMeters {
    meters:                  Option<Meters>,
    next_meter_id:           u16,
    keep_alive_meters_timer: std::time::Instant,
}

#[derive(Clone, Debug)]
pub struct WingConsole {
    rsock: Arc<Mutex<TcpStream>>,
    wsock: Arc<Mutex<TcpStream>>,
    main: Arc<Mutex<_WingConsoleMain>>,
    mtrs: Arc<Mutex<_WingConsoleMeters>>,

}

impl WingConsole {
    pub fn scan(stop_on_first: bool) -> Result<Vec<DiscoveryInfo>> {
        let dsock = UdpSocket::bind("0.0.0.0:0")?;
        dsock.set_broadcast(true)?;
        dsock.set_read_timeout(Some(Duration::from_millis(500)))
            .map_err(|e| Error::Io(e))?;

        let mut results = Vec::new();
        let mut attempts = 0;

        dsock.send_to(b"WING?", "255.255.255.255:2222")?;
        while attempts < 10 {
            let mut buf = [0u8; 1024];
            match dsock.recv_from(&mut buf) {
                Ok((received, _)) => {
                    if let Ok(response) = String::from_utf8(buf[..received].to_vec()) {
                        let tokens: Vec<&str> = response.split(',').collect();
                        if tokens.len() >= 6 && tokens[0] == "WING" {
                            results.push(DiscoveryInfo {
                                ip:       tokens[1].to_string(),
                                name:     tokens[2].to_string(),
                                model:    tokens[3].to_string(),
                                serial:   tokens[4].to_string(),
                                firmware: tokens[5].to_string(),
                            });
                            if stop_on_first {
                                break;
                            }
                        }
                    }
                }
                Err(_) => {
                    attempts += 1;
                }
            }
        }

        Ok(results)
    }

    pub fn connect(host_or_ip: Option<&str>) -> Result<Self> {
        let ip =
            if let Some(i) = host_or_ip {
                i.to_string()
            } else {
                let devices = WingConsole::scan(true)?;
                if !devices.is_empty() {
                    devices[0].ip.clone()
                } else {
                    return Err(Error::DiscoveryError);
                }
            };

        // Parse address first to validate it
        let addr = format!("{}:2222", ip);
        let socket_addr: std::net::SocketAddr = addr.parse()
            .map_err(|_| std::io::Error::new(
                std::io::ErrorKind::InvalidInput, 
                format!("Invalid IP address: {}", ip)
            ))?;
        
        // Use socket2 for better socket control
        let socket = socket2::Socket::new(
            if socket_addr.is_ipv4() {
                socket2::Domain::IPV4
            } else {
                socket2::Domain::IPV6
            },
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        // Enable TCP keepalive to detect broken connections
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(10))  // Start probing after 10 seconds
            .with_interval(Duration::from_secs(5));  // Probe every 5 seconds
        
        socket.set_tcp_keepalive(&keepalive)?;
        
        // Disable Nagle's algorithm for low latency
        socket.set_nodelay(true)?;
        
        // Enable SO_REUSEADDR to avoid "address already in use" errors
        socket.set_reuse_address(true)?;
        
        // Set connect timeout
        socket.set_read_timeout(Some(Duration::from_secs(10)))?;
        socket.set_write_timeout(Some(Duration::from_secs(5)))?;
        
        // Connect with timeout
        match socket.connect_timeout(&socket_addr.into(), Duration::from_secs(5)) {
            Ok(_) => {},
            Err(e) => {
                return Err(std::io::Error::new(
                    e.kind(),
                    format!("Failed to connect to Wing at {}: {} (check network, firewall, and that Wing is powered on)", ip, e)
                ).into());
            }
        }
        
        // Convert socket2::Socket to std::net::TcpStream
        let mut stream: TcpStream = socket.into();
        
        // Send initial handshake
        stream.write_all(&[0xdf, 0xd1])?;

        Ok(Self {
            wsock: Arc::new(Mutex::new(stream.try_clone()?)),
            rsock: Arc::new(Mutex::new(stream)),
            main: Arc::new(Mutex::new(_WingConsoleMain {
                keep_alive_timer: std::time::Instant::now() + std::time::Duration::from_secs(DATA_KEEP_ALIVE_SECONDS),
                rx_buf: [0; RX_BUFFER_SIZE],
                rx_buf_tail: 0,
                rx_buf_size: 0,
                rx_esc: false,
                rx_current_channel: -1,
                rx_has_in_pipe: None,
                current_node_id: 0,
            })),
            mtrs: Arc::new(Mutex::new(_WingConsoleMeters {
                keep_alive_meters_timer: std::time::Instant::now() + std::time::Duration::from_secs(METERS_KEEP_ALIVE_SECONDS),
                meters: None,
                next_meter_id: 0,
            })),
        })
    }

    pub fn read(&mut self) -> Result<WingResponse> {
        loop {
            let mainptr = self.main.clone();
            let mut main = mainptr.lock()
                .map_err(|_| Error::ConnectionError)?;
            let mut raw = Vec::new(); 
            let (ch, cmd) = self.decode_next(&mut main, &mut raw)?;
            
            // Process command based on protocol
            if cmd <= CMD_INT_INLINE_MAX {
                // Inline integer value (0-63)
                let v = cmd as i32;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_i32(v)));
            } else if cmd <= CMD_NODE_INDEX_MAX {
                // Node index request (64-127)
                // let v = cmd - 0x40 + 1;
                continue;
            } else if cmd <= CMD_SHORT_STRING_MAX {
                // Short string (128-191, length 1-64)
                let len = cmd - CMD_SHORT_STRING_MIN + 1;
                let v = self.read_string(&mut main, ch, len as usize, &mut raw)?;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_string(v)));
            } else if cmd <= CMD_MED_STRING_MAX {
                // Medium string (192-207, length 1-16)
                let len = cmd - CMD_MED_STRING_MIN + 1;
                let v = self.read_string(&mut main, ch, len as usize, &mut raw)?;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_string(v)));
            } else if cmd == CMD_EMPTY_STRING {
                // Empty string
                let v = String::new();
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_string(v)));
            } else if cmd == CMD_LONG_STRING {
                // Long string (up to 256 bytes)
                let len = self.read_u8(&mut main, ch, &mut raw)? + 1;
                let v = self.read_string(&mut main, ch, len as usize, &mut raw)?;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_string(v)));
            } else if cmd == CMD_NODE_INDEX_16 {
                // 16-bit node index
                let _v = self.read_u16(&mut main, ch, &mut raw)? + 1;
                continue;
            } else if cmd == CMD_INT16 {
                // 16-bit integer
                let v = self.read_i16(&mut main, ch, &mut raw)?;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_i16(v)));
            } else if cmd == CMD_INT32 {
                // 32-bit integer
                let v = self.read_i32(&mut main, ch, &mut raw)?;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_i32(v)));
            } else if cmd == CMD_FLOAT32 || cmd == CMD_FLOAT32_ALT {
                // 32-bit float
                let v = self.read_f(&mut main, ch, &mut raw)?;
                return Ok(WingResponse::NodeData(main.current_node_id, WingNodeData::with_float(v)));
            } else if cmd == CMD_SET_NODE_ID {
                // Set current node ID
                main.current_node_id = self.read_i32(&mut main, ch, &mut raw)?;
            } else if cmd == CMD_CLICK {
                // Click action - ignored
                continue;
            } else if cmd == CMD_STEP {
                // Step action - ignored
                let _v = self.read_i8(&mut main, ch, &mut raw)?;
                continue;
            } else if cmd == CMD_GOTO_ROOT {
                // Navigate to root - ignored
                continue;
            } else if cmd == CMD_GO_UP {
                // Navigate up - ignored
                continue;
            } else if cmd == CMD_REQUEST_DATA {
                // Request data - ignored
                continue;
            } else if cmd == CMD_REQUEST_DEF {
                // Request definition - ignored
                continue;
            } else if cmd == CMD_REQUEST_END {
                // End of request
                return Ok(WingResponse::RequestEnd);
            } else if cmd == CMD_NODE_DEF {
                // Node definition follows
                let def_len = self.read_u16(&mut main, ch, &mut raw)? as u32;
                if def_len == 0 { 
                    let _ = self.read_u32(&mut main, ch, &mut raw)?; 
                }
                raw.clear();
                for _ in 0..def_len { 
                    self.decode_next(&mut main, &mut raw)?; 
                } 
                return Ok(WingResponse::NodeDef(WingNodeDef::from_bytes(&raw)));
            }
        }
    }

    fn read_i8(&mut self, r: &mut _WingConsoleMain, _ch:i8, raw: &mut Vec::<u8>) -> Result<i8> {
        Ok(self.decode_next(r, raw)?.1 as i8)
    }
    fn read_u8(&mut self, r: &mut _WingConsoleMain, _ch:i8, raw: &mut Vec::<u8>) -> Result<u8> {
        Ok(self.decode_next(r, raw)?.1)
    }
    fn read_u16(&mut self, r: &mut _WingConsoleMain, _ch:i8, raw: &mut Vec::<u8>) -> Result<u16> {
        let a = self.decode_next(r, raw)?;
        let b = self.decode_next(r, raw)?;
        Ok(((a.1 as u16) << 8) | b.1 as u16)
    }
    fn read_i16(&mut self, r: &mut _WingConsoleMain, ch:i8, raw: &mut Vec::<u8>) -> Result<i16> {
        Ok(self.read_u16(r, ch, raw)? as i16)
    }
    fn read_u32(&mut self, r: &mut _WingConsoleMain, _ch:i8, raw: &mut Vec::<u8>) -> Result<u32> {
        let a = self.decode_next(r, raw)?;
        let b = self.decode_next(r, raw)?;
        let c = self.decode_next(r, raw)?;
        let d = self.decode_next(r, raw)?;
        Ok(
            ((a.1 as u32) << 24) |
            ((b.1 as u32) << 16) |
            ((c.1 as u32) << 8) |
            d.1 as u32
            )
    }
    fn read_i32(&mut self, r: &mut _WingConsoleMain, ch:i8, raw: &mut Vec::<u8>) -> Result<i32> {
        Ok(self.read_u32(r, ch, raw)? as i32)
    }

    fn read_string(&mut self, r: &mut _WingConsoleMain, _ch:i8, len:usize, raw: &mut Vec::<u8>) -> Result<String> {
        // define u8 array of size len and fill it with decode_next
        let buf = (0..len).map(|_| self.decode_next(r, raw).map(|(_, v)| v)).collect::<Result<Vec<u8>>>()?;
        // convert u8 array to string
        String::from_utf8(buf).map_err(|_| Error::InvalidData)
    }

    fn read_f(&mut self, r: &mut _WingConsoleMain, _ch:i8, raw: &mut Vec::<u8>) -> Result<f32> {
        let a = self.decode_next(r, raw)?;
        let b = self.decode_next(r, raw)?;
        let c = self.decode_next(r, raw)?;
        let d = self.decode_next(r, raw)?;
        let val = ((a.1 as u32) << 24) |
            ((b.1 as u32) << 16) |
            ((c.1 as u32) << 8) |
            d.1 as u32;
        Ok(f32::from_bits(val))
    }

    /// read() will call this as needed, but if you don't call read() then the Wing Console will
    /// hang up the connection after a 10 seconds of no activity. You should call this yourself
    /// periodically if you are not calling read().
    pub fn keep_alive(&mut self) -> Result<()> {
        let mainptr = self.main.clone();
        let mut main = mainptr.lock()
            .map_err(|_| Error::ConnectionError)?;
        self._keep_alive(&mut main)
    }

    fn _keep_alive(&mut self, r: &mut _WingConsoleMain) -> Result<()> {
        if r.keep_alive_timer <= std::time::Instant::now() {
            // println!("keep_alive");
            self.wsock.clone().lock()
                .map_err(|_| Error::ConnectionError)?
                .write_all(&[0xdf, 0xd1])?;
            r.keep_alive_timer = std::time::Instant::now() + std::time::Duration::from_secs(DATA_KEEP_ALIVE_SECONDS);
        }
        Ok(())
    }

    /// read_meters() will call this as needed, but if you don't call read_meters() then the Wing Console will
    /// hang up the connection after a 5 seconds of no activity. You should call this yourself
    /// periodically if you are not calling read_meters().
    pub fn keep_alive_meters(&mut self) -> Result<()> {
        let mtrsptr = self.mtrs.clone();
        let mut mtrs = mtrsptr.lock()
            .map_err(|_| Error::ConnectionError)?;
        self._keep_alive_meters(&mut mtrs)
    }

    fn _keep_alive_meters(&mut self, m: &mut _WingConsoleMeters) -> Result<()> {
        if m.keep_alive_meters_timer <= std::time::Instant::now() {
            // println!("keep_alive_meters");
            let meters = m.meters.as_ref().ok_or(Error::ConnectionError)?;
            let mut keepalive = [
                0xdf, 0xd3, 0xd4,
                0x00,
                0x00,
                ((meters.port >> 8) & 0xff) as u8,
                (meters.port & 0xff) as u8,
                0xdf, 0xd1
            ];
            let mut i = m.next_meter_id as i32;
            while i > 0 {
                keepalive[3] = ((i >> 8) & 0xff) as u8;
                keepalive[4] = (i & 0xff) as u8;
                self.wsock.clone().lock()
                    .map_err(|_| Error::ConnectionError)?
                    .write_all(&keepalive)?;
                i -= 1;
            }
            m.keep_alive_meters_timer = std::time::Instant::now() + std::time::Duration::from_secs(METERS_KEEP_ALIVE_SECONDS);
        }
        Ok(())
    }

    fn decode_next(&mut self, r: &mut _WingConsoleMain, raw: &mut Vec::<u8>) -> Result<(i8, u8)> {
        if let Some(value) = r.rx_has_in_pipe.take() {
            // println!("has in pipe");
            raw.push(value);
            return Ok((r.rx_current_channel, value));
        }

        loop {
            self._keep_alive(r)?;
            if r.rx_buf_size == 0 {
                // Calculate timeout safely to avoid panic if time has passed
                let timeout = r.keep_alive_timer
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or(Duration::from_millis(10));
                
                self.rsock.clone().lock()
                    .map_err(|_| Error::ConnectionError)?
                    .set_read_timeout(Some(timeout))?;
                    
                match self.rsock.clone().lock()
                    .map_err(|_| Error::ConnectionError)?
                    .read(&mut r.rx_buf) {
                    Ok(n) if n > 0 => {
                        // println!("got n {}...", n);
                        r.rx_buf_size = n;
                        r.rx_buf_tail = 0;
                    }
                    // check for blocking error
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Ok(_) => return Err(Error::ConnectionError),
                    Err(e) => return Err(e.into()),
                }
            }

            let byte = r.rx_buf[r.rx_buf_tail];
            // println!("rx_buf_tail: {}, rx_buf_size: {}, byte: {:X} buf: {}",
            //     self.rx_buf_tail,
            //     self.rx_buf_size, byte,
            //     self.rx_buf.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(","));
            r.rx_buf_tail += 1;
            r.rx_buf_size -= 1;

            if ! r.rx_esc {
                if byte == 0xdf {
                    r.rx_esc = true;
                } else {
                    raw.push(byte);
                    break Ok((r.rx_current_channel, byte))
                }
            } else if byte == 0xdf {
                break Ok((r.rx_current_channel, byte))
            } else {
                r.rx_esc = false;
                if byte == 0xde {
                    raw.push(0xdf);
                    break Ok((r.rx_current_channel, 0xdf))
                } else if (0xd0..0xde).contains(&byte) {
                    r.rx_current_channel = (byte - 0xd0) as i8;
                    continue;
                } else if r.rx_current_channel >= 0 {
                    r.rx_has_in_pipe = Some(byte);
                    raw.push(0xdf);
                    break Ok((r.rx_current_channel, 0xdf))
                } else {
                    raw.push(byte);
                    break Ok((r.rx_current_channel, byte))
                }
            }
        }
    }

    fn format_id(&self, id: i32, buf: &mut Vec<u8>, prefix: u8, suffix: Option<u8>) {
        buf.push(prefix);

        let b1 = ((id >> 24) & 0xFF) as u8;
        let b2 = ((id >> 16) & 0xFF) as u8;
        let b3 = ((id >>  8) & 0xFF) as u8;
        let b4 = ((id      ) & 0xFF) as u8;

        buf.push(b1); if b1 == ESCAPE_BYTE { buf.push(ESCAPED_ESCAPE); }
        buf.push(b2); if b2 == ESCAPE_BYTE { buf.push(ESCAPED_ESCAPE); }
        buf.push(b3); if b3 == ESCAPE_BYTE { buf.push(ESCAPED_ESCAPE); }
        buf.push(b4); if b4 == ESCAPE_BYTE { buf.push(ESCAPED_ESCAPE); }

        if let Some(suffix1) = suffix {
            buf.push(suffix1);
        }
    }

    pub fn request_node_definition(&mut self, id: i32) -> Result<()> {
        let mut buf = Vec::new();
        if id == 0 {
            buf.push(0xda);
            buf.push(0xdd);
        } else {
            self.format_id(id, &mut buf, 0xd7, Some(0xdd));
        };
        self.wsock.clone().lock()
            .map_err(|_| Error::ConnectionError)?
            .write_all(&buf)?;
        Ok(())
    }

    pub fn request_node_data(&mut self, id: i32) -> Result<()> {
        let mut buf = Vec::new();
        if id == 0 {
            buf.push(0xda);
            buf.push(0xdc);
        } else {
            self.format_id(id, &mut buf, 0xd7, Some(0xdc));
        };
        self.wsock.clone().lock()
            .map_err(|_| Error::ConnectionError)?
            .write_all(&buf)?;
        Ok(())
    }


    /// Subscribes to meters from the Wing mixer and returns a meter ID that can be used to
    /// associate the values that come back when you call read_meter()
    pub fn request_meter(&mut self, meters: &[Meter]) -> Result<u16>
    {
        let mtrsptr = self.mtrs.clone();
        let mut mtrs = mtrsptr.lock()
            .map_err(|_| Error::ConnectionError)?;
        mtrs.next_meter_id += 1;

        if mtrs.meters.is_none() {
            let socket = UdpSocket::bind("0.0.0.0:0")?;
            let port = socket.local_addr()?.port();
            socket.set_read_timeout(Some(Duration::from_millis(1000)))
                .map_err(|e| Error::Io(e))?;
            mtrs.meters = Some(Meters { socket, port });
        } else {
            self._keep_alive_meters(&mut mtrs)?;
        }
        let md = mtrs.meters.as_ref()
            .ok_or(Error::ConnectionError)?;

        let mut buf = vec![
            0xdf, 0xd3,
            0xd3,
            ((md.port >> 8) & 0xff) as u8,
            (md.port & 0xff) as u8,
            0xd4,
            ((mtrs.next_meter_id >> 8) & 0xff) as u8,
            (mtrs.next_meter_id & 0xff) as u8,
            ((md.port >> 8) & 0xff) as u8,
            (md.port & 0xff) as u8,
            0xdc,
        ];

        for meter in meters {
            match meter {
                Meter::Channel(n) => {
                    buf.push(0xa0);
                    buf.push(*n);
                }
                Meter::Aux(n) => {
                    buf.push(0xa1);
                    buf.push(*n);
                }
                Meter::Bus(n) => {
                    buf.push(0xa2);
                    buf.push(*n);
                }
                Meter::Main(n) => {
                    buf.push(0xa3);
                    buf.push(*n);
                }
                Meter::Matrix(n) => {
                    buf.push(0xa4);
                    buf.push(*n);
                }
                Meter::Dca(n) => {
                    buf.push(0xa5);
                    buf.push(*n);
                }
                Meter::Fx(n) => {
                    buf.push(0xa6);
                    buf.push(*n);
                }
                Meter::Source(n) => {
                    buf.push(0xa7);
                    buf.push(*n);
                }
                Meter::Output(n) => {
                    buf.push(0xa8);
                    buf.push(*n);
                }
                Meter::Monitor => {
                    buf.push(0xa9);
                }
                Meter::Rta => {
                    buf.push(0xaa);
                }
                Meter::Channel2(n) => {
                    buf.push(0xab);
                    buf.push(*n);
                }
                Meter::Aux2(n) => {
                    buf.push(0xac);
                    buf.push(*n);
                }
                Meter::Bus2(n) => {
                    buf.push(0xad);
                    buf.push(*n);
                }
                Meter::Main2(n) => {
                    buf.push(0xae);
                    buf.push(*n);
                }
                Meter::Matrix2(n) => {
                    buf.push(0xaf);
                    buf.push(*n);
                }
            }
        }

        buf.push(0xde); // end of def
        buf.push(0xdf);
        buf.push(0xd1);

        self.wsock.clone().lock()
            .map_err(|_| Error::ConnectionError)?
            .write_all(&buf)?;

        Ok(mtrs.next_meter_id)
    }

    /// reads any meter values that have been requested with request_meter() and returns the meter
    /// ID along with the meters values
    pub fn read_meters(&mut self) -> Result<(u16, Vec<i16>)> {
        loop {
            let mptr = self.mtrs.clone();
            let mut m = mptr.lock()
                .map_err(|_| Error::ConnectionError)?;

            self._keep_alive_meters(&mut m)?;
            let md = m.meters.as_ref()
                .ok_or(Error::ConnectionError)?;
            let mut buf = [0u8; 8192];
            
            // Calculate timeout safely to avoid panic
            let timeout = m.keep_alive_meters_timer
                .checked_duration_since(std::time::Instant::now())
                .unwrap_or(Duration::from_millis(10));
            md.socket.set_read_timeout(Some(timeout))?;
            match md.socket.recv_from(&mut buf) {
                Ok((received, _addr)) => {
                    return Ok((u16::from_be_bytes([buf[0], buf[1]]), buf[4..received]
                            .chunks_exact(2) // Take 2 bytes at a time
                            .map(|chunk| i16::from_be_bytes([chunk[0], chunk[1]]))
                            .collect()));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(_) => {
                    return Err(Error::ConnectionError);
                }
            }
        }
    }

    pub fn set_string(&mut self, id: i32, value: &str) -> Result<()> {
        let mut buf = Vec::new();
        self.format_id(id, &mut buf, 0xd7, None);

        if value.is_empty() {
            buf.push(0xd0);
        } else if value.len() <= 64 {
            buf.push(0x7f + value.len() as u8);
        } else if value.len() <= 256 {
            buf.push(0xd1);
            buf.push((value.len()-1) as u8);
        }

        for c in value.bytes() {
            buf.push(c);
            // do we need this escaping? i guess 0xdf never really shows up in strings unless its
            // unicode stuff that the wing probably doesn't support
            // if c == 0xdf { buf.push(0xde); }
        }
        self.wsock.clone().lock().unwrap().write_all(&buf)?;
        Ok(())
    }

    pub fn set_float(&mut self, id: i32, value: f32) -> Result<()> {
        let mut buf = Vec::new();
        self.format_id(id, &mut buf, 0xd7, Some(0xd5));

        let bytes = value.to_be_bytes();
        buf.push(bytes[0]);
        buf.push(bytes[1]);
        buf.push(bytes[2]);
        buf.push(bytes[3]);

        self.wsock.clone().lock()
            .map_err(|_| Error::ConnectionError)?
            .write_all(&buf)?;
        Ok(())
    }

    pub fn set_int(&mut self, id: i32, value: i32) -> Result<()> {
        let mut buf = Vec::new();
        self.format_id(id, &mut buf, 0xd7, None);

        let bytes = value.to_be_bytes();

        if (0..=0x3f).contains(&value) {
            buf.push(value as u8);
        } else if (-32768..=32767).contains(&value) {
            buf.push(0xd3);
            buf.push(bytes[0]);
            buf.push(bytes[1]);
        } else {
            buf.push(0xd4);
            buf.push(bytes[0]);
            buf.push(bytes[1]);
            buf.push(bytes[2]);
            buf.push(bytes[3]);
        }

        self.wsock.clone().lock()
            .map_err(|_| Error::ConnectionError)?
            .write_all(&buf)?;
        Ok(())
    }

    pub fn name_to_id(fullname: &str) -> Option<i32> {
        if let Ok(num) = fullname.parse::<i32>() {
            Some(num)
        } else {
            NAME_TO_DEF.get(fullname).map(|x| x.id)
        }
    }
    pub fn name_to_def(fullname: &str) -> Option<&WingNodeDef> {
        NAME_TO_DEF.get(fullname)
    }

    pub fn id_to_defs(id: i32) -> Option<Vec<(String, WingNodeDef)>> {
        ID_TO_NAME.get(&id)
            .cloned()
            .map(|names|
                names
                .iter()
                .map(|n| (n, NAME_TO_DEF.get(n)))
                .filter(|x| x.1.is_some())
                .filter_map(|(n, v)| v.map(|def| (n.clone(), def.clone())))
                .collect())
    }
}

impl Drop for WingConsole {
    fn drop(&mut self) {
        // Gracefully shutdown sockets, ignore errors during cleanup
        if let Ok(sock) = self.wsock.clone().lock() {
            let _ = sock.shutdown(std::net::Shutdown::Both);
        }
        if let Ok(sock) = self.rsock.clone().lock() {
            let _ = sock.shutdown(std::net::Shutdown::Both);
        }
    }
}
