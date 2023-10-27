use hdf5::types::FixedAscii;
use hdf5::H5Type;

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[allow(non_camel_case_types)]
#[repr(i8)]
pub enum Direction {
    CLIENT_TO_SERVER = 1,
    SERVER_TO_CLIENT = -1,
    PADDING = 0,
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

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct Cell {
    pub time: f64,
    pub direction: Direction,
    pub cell_cmd: CellCommand,
    pub relay_cmd: RelayCommand,
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
