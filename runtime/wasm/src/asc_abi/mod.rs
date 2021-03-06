pub use self::asc_ptr::AscPtr;
use std::mem::size_of;
use std::slice;
use wasmi;

pub mod asc_ptr;
pub mod class;
#[cfg(test)]
mod test;

///! Facilities for creating and reading objects on the memory of an
///! AssemblyScript (Asc) WASM module. Objects are passed through
///! the `asc_new` and `asc_get` methods of an `AscHeap` implementation.
///! These methods take types that implement `To`/`FromAscObj` and are
///! therefore convertible to/from an `AscType`.
///! Implementations of `AscType` live in the `class` module.
///! Implementations of `To`/`FromAscObj` live in the `to_from` module.

/// WASM is little-endian, and for simplicity we currently assume that the host
/// is also little-endian.
#[cfg(target_endian = "big")]
compile_error!("big-endian targets are currently unsupported");

/// A type that can read and write to the Asc heap. Call `asc_new` and `asc_get`
/// for reading and writing Rust structs from and to Asc.
///
/// The implementor must provide the direct Asc interface with `raw_new` and `get`.
pub trait AscHeap: Sized {
    /// Allocate new space and write `bytes`, return the allocated address.
    fn raw_new(&self, bytes: &[u8]) -> Result<u32, wasmi::Error>;

    /// Just like `wasmi::MemoryInstance::get`.
    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, wasmi::Error>;

    /// Instatiate `rust_obj` as an Asc object of class `C`.
    /// Returns a pointer to the Asc heap.
    ///
    /// This operation is expensive as it requires a call to `raw_new` for every
    /// nested object.
    fn asc_new<C, T: ?Sized>(&self, rust_obj: &T) -> AscPtr<C>
    where
        C: AscType,
        T: ToAscObj<C>,
    {
        AscPtr::alloc_obj(&rust_obj.to_asc_obj(self), self)
    }

    ///  Read the rust representation of an Asc object of class `C`.
    ///
    ///  This operation is expensive as it requires a call to `get` for every
    ///  nested object.
    fn asc_get<T, C>(&self, asc_ptr: AscPtr<C>) -> T
    where
        C: AscType,
        T: FromAscObj<C>,
    {
        T::from_asc_obj(asc_ptr.read_ptr(self), self)
    }
}

/// Type that can be converted to an Asc object of class `C`.
pub trait ToAscObj<C: AscType> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> C;
}

/// Type that can be converted from an Asc object of class `C`.
pub trait FromAscObj<C: AscType> {
    fn from_asc_obj<H: AscHeap>(obj: C, heap: &H) -> Self;
}

// `AscType` is not really public, implementors should live inside the `class` module.

/// A type that has a direct corespondence to an Asc type, which
/// should be documented in a comment.
///
/// The default impl will memcopy the value as bytes, which is suitable for
/// structs that are `#[repr(C)]` and whose fields are all `AscValue`,
/// or Rust enums that are `#[repr(u32)]`.
/// Special classes like `ArrayBuffer` use custom impls.
pub trait AscType: Sized {
    /// Transform the Rust representation of this instance into an sequence of
    /// bytes that is precisely the memory layout of a corresponding Asc instance.
    fn to_asc_bytes(&self) -> Vec<u8> {
        let erased_self = (self as *const Self) as *const u8;
        let self_size = size_of::<Self>();

        // Cast the byte array as a reference to self, and copy it to a `Vec`.
        // While technically unspecified, this is almost just a memcopy and is
        // expected be specified as safe behaviour, see rust-lang/rust#30500.
        unsafe { slice::from_raw_parts(erased_self, self_size) }.to_vec()
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        let asc_obj_as_self = asc_obj.as_ptr() as *const Self;

        // Safe because `u8` is `Copy`. Also see notes on `to_asc_bytes`.
        unsafe { ::std::ptr::read_unaligned(asc_obj_as_self) }
    }

    /// Size of the corresponding Asc instance in bytes.
    fn asc_size<H: AscHeap>(_ptr: AscPtr<Self>, _heap: &H) -> u32 {
        size_of::<Self>() as u32
    }
}

// `AscValue` also isn't really public.

/// An Asc primitive or an `AscPtr` into the Asc heap. A type marked as
/// `AscValue` must have the same byte representation in Rust and Asc, including
/// same size, and therefore use the default impl of `AscType`. Also, size must
/// be equal to alignment.
pub trait AscValue: AscType + Copy + Default {}

impl<T> AscType for AscPtr<T> {}
impl AscType for bool {}
impl AscType for i8 {}
impl AscType for i16 {}
impl AscType for i32 {}
impl AscType for i64 {}
impl AscType for u8 {}
impl AscType for u16 {}
impl AscType for u32 {}
impl AscType for u64 {}
impl AscType for f32 {}
impl AscType for f64 {}

impl<T> AscValue for AscPtr<T> {}
impl AscValue for bool {}
impl AscValue for i8 {}
impl AscValue for i16 {}
impl AscValue for i32 {}
impl AscValue for i64 {}
impl AscValue for u8 {}
impl AscValue for u16 {}
impl AscValue for u32 {}
impl AscValue for u64 {}
impl AscValue for f32 {}
impl AscValue for f64 {}
