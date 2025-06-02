use hdf5::types::{FixedAscii, StringError, VarLenArray};
use hdf5::H5Type;

/// The direction that the cell was traveling.
#[derive(H5Type, Clone, Copy, Debug, Eq, PartialEq)]
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

/// The control command from a Tor cell.
/// 
/// See https://spec.torproject.org/tor-spec/cell-packet-format.html
#[derive(H5Type, Clone, Copy, Debug, Eq, PartialEq)]
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

/// The control (sub)command of a Tor Relay-type cell.
/// 
/// See: https://spec.torproject.org/tor-spec/relay-cells.html
#[derive(H5Type, Clone, Copy, Debug, Eq, PartialEq)]
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
    /// A custom cell type used solely in the GTT23 measurement project.
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

/// The meta-data associated with a Cell observed by a Tor relay.
#[derive(H5Type, Clone, Copy, Debug)]
#[repr(C)]
pub struct Cell {
    pub time: f64,
    pub direction: Direction,
    pub cell_cmd: CellCommand,
    pub relay_cmd: RelayCommand,
}

impl Cell {
    /// Creates an empty `Cell` with all meta-data zeroed out.
    pub fn empty() -> Self {
        Self {
            time: 0.0,
            direction: Direction::PADDING,
            cell_cmd: CellCommand::PADDING,
            relay_cmd: RelayCommand::NOT_PRESENT,
        }
    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
            && self.direction == other.direction
            && self.cell_cmd == other.cell_cmd
            && self.relay_cmd == other.relay_cmd
    }
}

impl Eq for Cell {}

impl PartialOrd for Cell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.total_cmp(&other.time)
    }
}

/// The meta-data associated with a Circuit observed by a Tor relay.
#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct Circuit {
    /// A unique ID.
    pub uuid: FixedAscii<32>,
    /// The initial first-party domain looked up on the circuit.
    pub domain: FixedAscii<44>,
    /// The same as `domain`, but passed through `libpsl` to get the domain's
    /// shortest private suffix.
    /// 
    /// See: https://rockdaboot.github.io/libpsl/libpsl-Public-Suffix-List-functions.html#psl-registrable-domain
    pub shortest_private_suffix: FixedAscii<44>,
    /// An integer representing the day of measurement.
    pub day: u8,
    /// An integer representing the port number used to connect to the external
    /// server.
    pub port: u16,
    /// The number of cells observed on the circuit, representing the valid part
    /// of the `cells` array.
    pub len: u16,
    /// The cells observed on the circuit. Only `cells[0..len]` are valid, the
    /// rest are padding.
    pub cells: [Cell; 5000],
}

impl Circuit {
    /// Creates an empty `Circuit` with all meta-data zeroed out.
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

    /// A string that can be used as a label for this circuit.
    pub fn label(&self) -> FixedAscii<44> {
        if self.shortest_private_suffix.is_empty() {
            self.domain
        } else {
            self.shortest_private_suffix
        }
    }
}

/// A modified version of a Tor circuit used for augmentation purposes.
#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct AugmentedCircuit {
    pub uuid: FixedAscii<32>,
    /// The UUID of the `Circuit` from which this `AugmentedCircuit` was created.
    pub uuid_gtt23: FixedAscii<32>,
    /// An integer that allows linking many augmented circuits to the same GTT23 circuit.
    pub aug_index: u16,
    /// The same meaning as `Circuit.len`.
    pub len: u16,
    /// The same meaning as `Circuit.cells`.
    pub cells: [Cell; 5000],
}

impl AugmentedCircuit {
    /// Creates an empty `AugmentedCircuit` with all meta-data zeroed out.
    pub fn empty() -> Self {
        Self {
            uuid: fixedascii_null::<32>().unwrap(),
            uuid_gtt23: fixedascii_null::<32>().unwrap(),
            aug_index: 0,
            len: 0,
            cells: [Cell::empty(); 5000],
        }
    }
}

/// An integer index into an array of Circuits. Requires that the length of the
/// Circuit array is less than 2**32.
pub type CircuitIndex = u32;

#[derive(H5Type, Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct IndexEntry<T: H5Type> {
    /// The value being indexed. For example, in the uuid index, this would be
    /// the `FixedAscii<32>` uuid string.
    pub value: T,
    /// The index of the circuit in the circuits dataset to which this entry's
    /// value uniquely corresponds. For example, in the uuid index, the circuit
    /// with the uuid will appear in the circuits dataset at this index (i.e.,
    /// array offset).
    pub index: CircuitIndex,
}

#[derive(H5Type, Clone, PartialEq, Debug)]
#[repr(C)]
pub struct IndexArrayEntry<T: H5Type> {
    /// The value being indexed. For example, in the day index this would be the
    /// `u8` day integer.
    pub value: T,
    /// An array containing the indices of the circuits in the circuits dataset
    /// that have this entry's value. For example, in the day index, the indices
    /// of all circuits with the day value will be stored in this array.
    pub indexarr: VarLenArray<CircuitIndex>,
}

/// A helper to converts `s` to a FixedAscii type, truncating `s` or
/// right-padding with 0x0 to meet the desired fixed length.
pub fn fixedascii_from_str<const N: usize>(s: &str) -> Result<FixedAscii<N>, StringError> {
    let pad = format!("{s:\0<width$}", width = N);
    let pad_then_trunc = &pad[0..N];
    FixedAscii::<N>::from_ascii(pad_then_trunc)
}


/// A helper to create an empty FixedAscii string.
pub fn fixedascii_null<const N: usize>() -> Result<FixedAscii<N>, StringError> {
    fixedascii_from_str::<N>("")
}
