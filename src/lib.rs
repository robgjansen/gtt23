use hdf5::types::{FixedAscii, StringError};
use hdf5::H5Type;

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[allow(non_camel_case_types)]
#[repr(i8)]
pub enum Direction {
    CLIENT_TO_SERVER = 1,
    SERVER_TO_CLIENT = -1,
    PADDING = 0,
}

impl TryFrom<i8> for Direction {
    type Error = String;

    fn try_from(v: i8) -> Result<Self, Self::Error> {
        match v {
            v if v == Direction::CLIENT_TO_SERVER as i8 => Ok(Direction::CLIENT_TO_SERVER),
            v if v == Direction::SERVER_TO_CLIENT as i8 => Ok(Direction::SERVER_TO_CLIENT),
            v if v == Direction::PADDING as i8 => Ok(Direction::PADDING),
            _ => Err(format!("Unexpected direction value {v}").to_string()),
        }
    }
}

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[allow(non_camel_case_types)]
#[repr(u8)]
pub enum CellCommand {
    PADDING = 0,
    CREATE = 1,
    CREATED = 2,
    RELAY = 3,
    DESTROY = 4,
    CREATE_FAST = 5,
    CREATED_FAST = 6,
    VERSIONS = 7,
    NETINFO = 8,
    RELAY_EARLY = 9,
    CREATE2 = 10,
    CREATED2 = 11,
    PADDING_NEGOTIATE = 12,
    VPADDING = 128,
    CERTS = 129,
    AUTH_CHALLENGE = 130,
    AUTHENTICATE = 131,
    AUTHORIZE = 132,
}

impl TryFrom<u8> for CellCommand {
    type Error = String;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            v if v == CellCommand::PADDING as u8 => Ok(CellCommand::PADDING),
            v if v == CellCommand::CREATE as u8 => Ok(CellCommand::CREATE),
            v if v == CellCommand::CREATED as u8 => Ok(CellCommand::CREATED),
            v if v == CellCommand::RELAY as u8 => Ok(CellCommand::RELAY),
            v if v == CellCommand::DESTROY as u8 => Ok(CellCommand::DESTROY),
            v if v == CellCommand::CREATE_FAST as u8 => Ok(CellCommand::CREATE_FAST),
            v if v == CellCommand::CREATED_FAST as u8 => Ok(CellCommand::CREATED_FAST),
            v if v == CellCommand::VERSIONS as u8 => Ok(CellCommand::VERSIONS),
            v if v == CellCommand::NETINFO as u8 => Ok(CellCommand::NETINFO),
            v if v == CellCommand::RELAY_EARLY as u8 => Ok(CellCommand::RELAY_EARLY),
            v if v == CellCommand::CREATE2 as u8 => Ok(CellCommand::CREATE2),
            v if v == CellCommand::CREATED2 as u8 => Ok(CellCommand::CREATED2),
            v if v == CellCommand::PADDING_NEGOTIATE as u8 => Ok(CellCommand::PADDING_NEGOTIATE),
            v if v == CellCommand::VPADDING as u8 => Ok(CellCommand::VPADDING),
            v if v == CellCommand::CERTS as u8 => Ok(CellCommand::CERTS),
            v if v == CellCommand::AUTH_CHALLENGE as u8 => Ok(CellCommand::AUTH_CHALLENGE),
            v if v == CellCommand::AUTHENTICATE as u8 => Ok(CellCommand::AUTHENTICATE),
            v if v == CellCommand::AUTHORIZE as u8 => Ok(CellCommand::AUTHORIZE),
            _ => Err(format!("Unexpected cell command value {v}").to_string()),
        }
    }
}

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[allow(non_camel_case_types)]
#[repr(u8)]
pub enum RelayCommand {
    NOT_PRESENT = 0,
    BEGIN = 1,
    DATA = 2,
    END = 3,
    CONNECTED = 4,
    SENDME = 5,
    EXTEND = 6,
    EXTENDED = 7,
    TRUNCATE = 8,
    TRUNCATED = 9,
    DROP = 10,
    RESOLVE = 11,
    RESOLVED = 12,
    BEGIN_DIR = 13,
    EXTEND2 = 14,
    EXTENDED2 = 15,
    SIGNAL = 16,
    ESTABLISH_INTRO = 32,
    ESTABLISH_RENDEZVOUS = 33,
    INTRODUCE1 = 34,
    INTRODUCE2 = 35,
    RENDEZVOUS1 = 36,
    RENDEZVOUS2 = 37,
    INTRO_ESTABLISHED = 38,
    RENDEZVOUS_ESTABLISHED = 39,
    INTRODUCE_ACK = 40,
    PADDING_NEGOTIATE = 41,
    PADDING_NEGOTIATED = 42,
    XOFF = 43,
    XON = 44,
}

impl TryFrom<u8> for RelayCommand {
    type Error = String;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            v if v == RelayCommand::NOT_PRESENT as u8 => Ok(RelayCommand::NOT_PRESENT),
            v if v == RelayCommand::BEGIN as u8 => Ok(RelayCommand::BEGIN),
            v if v == RelayCommand::DATA as u8 => Ok(RelayCommand::DATA),
            v if v == RelayCommand::END as u8 => Ok(RelayCommand::END),
            v if v == RelayCommand::CONNECTED as u8 => Ok(RelayCommand::CONNECTED),
            v if v == RelayCommand::SENDME as u8 => Ok(RelayCommand::SENDME),
            v if v == RelayCommand::EXTEND as u8 => Ok(RelayCommand::EXTEND),
            v if v == RelayCommand::EXTENDED as u8 => Ok(RelayCommand::EXTENDED),
            v if v == RelayCommand::TRUNCATE as u8 => Ok(RelayCommand::TRUNCATE),
            v if v == RelayCommand::TRUNCATED as u8 => Ok(RelayCommand::TRUNCATED),
            v if v == RelayCommand::DROP as u8 => Ok(RelayCommand::DROP),
            v if v == RelayCommand::RESOLVE as u8 => Ok(RelayCommand::RESOLVE),
            v if v == RelayCommand::RESOLVED as u8 => Ok(RelayCommand::RESOLVED),
            v if v == RelayCommand::BEGIN_DIR as u8 => Ok(RelayCommand::BEGIN_DIR),
            v if v == RelayCommand::EXTEND2 as u8 => Ok(RelayCommand::EXTEND2),
            v if v == RelayCommand::EXTENDED2 as u8 => Ok(RelayCommand::EXTENDED2),
            v if v == RelayCommand::SIGNAL as u8 => Ok(RelayCommand::SIGNAL),
            v if v == RelayCommand::ESTABLISH_INTRO as u8 => Ok(RelayCommand::ESTABLISH_INTRO),
            v if v == RelayCommand::ESTABLISH_RENDEZVOUS as u8 => {
                Ok(RelayCommand::ESTABLISH_RENDEZVOUS)
            }
            v if v == RelayCommand::INTRODUCE1 as u8 => Ok(RelayCommand::INTRODUCE1),
            v if v == RelayCommand::INTRODUCE2 as u8 => Ok(RelayCommand::INTRODUCE2),
            v if v == RelayCommand::RENDEZVOUS1 as u8 => Ok(RelayCommand::RENDEZVOUS1),
            v if v == RelayCommand::RENDEZVOUS2 as u8 => Ok(RelayCommand::RENDEZVOUS2),
            v if v == RelayCommand::INTRO_ESTABLISHED as u8 => Ok(RelayCommand::INTRO_ESTABLISHED),
            v if v == RelayCommand::RENDEZVOUS_ESTABLISHED as u8 => {
                Ok(RelayCommand::RENDEZVOUS_ESTABLISHED)
            }
            v if v == RelayCommand::INTRODUCE_ACK as u8 => Ok(RelayCommand::INTRODUCE_ACK),
            v if v == RelayCommand::PADDING_NEGOTIATE as u8 => Ok(RelayCommand::PADDING_NEGOTIATE),
            v if v == RelayCommand::PADDING_NEGOTIATED as u8 => {
                Ok(RelayCommand::PADDING_NEGOTIATED)
            }
            v if v == RelayCommand::XOFF as u8 => Ok(RelayCommand::XOFF),
            v if v == RelayCommand::XON as u8 => Ok(RelayCommand::XON),
            _ => Err(format!("Unexpected relay command value {v}").to_string()),
        }
    }
}

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct Cell {
    pub time: f64,
    pub direction: Direction,
    pub cell_cmd: CellCommand,
    pub relay_cmd: RelayCommand,
}

impl Cell {
    pub fn empty() -> Self {
        Self {
            time: 0.0,
            direction: Direction::PADDING,
            cell_cmd: CellCommand::PADDING,
            relay_cmd: RelayCommand::NOT_PRESENT,
        }
    }
}

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct Circuit {
    pub uuid: FixedAscii<32>,
    pub domain: FixedAscii<44>,
    pub shortest_private_suffix: FixedAscii<44>,
    pub day: u8,
    pub port: u16,
    pub len: u16,
    pub cells: [Cell; 5000],
}

impl Circuit {
    pub fn empty() -> Self {
        Self {
            uuid: fixedascii_null::<32>().unwrap(),
            domain: fixedascii_null::<44>().unwrap(),
            shortest_private_suffix: fixedascii_null::<44>().unwrap(),
            day: 0,
            port: 0,
            len: 0,
            cells: [Cell::empty(); 5000],
        }
    }
}

/// Converts `s` to a FixedAscii type, truncating `s` or right-padding with
/// 0x0 to meet the desired fixed length.
pub fn fixedascii_from_str<const N: usize>(s: &str) -> Result<FixedAscii<N>, StringError> {
    let pad = format!("{s:\0<width$}", width = N);
    let pad_then_trunc = &pad[0..N];
    FixedAscii::<N>::from_ascii(pad_then_trunc)
}

pub fn fixedascii_null<const N: usize>() -> Result<FixedAscii<N>, StringError> {
    fixedascii_from_str::<N>("")
}
