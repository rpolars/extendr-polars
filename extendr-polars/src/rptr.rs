use extendr_api::prelude::Robj;
//handle pointers and ownership between R and rust

/// convert an Robj of a string (10-base) of numbers to a usize value
/// Useful as R has no 64bit integer type
pub fn robj_str_ptr_to_usize(robj: &Robj) -> std::result::Result<usize, String> {
    let str: &str = robj.as_str().ok_or("robj str ptr not a str".to_string())?;
    let us: usize = str.parse().map_err(|err| format!("parse error : {err}"))?;
    Ok(us)
}

/// convert usize to a Robj of string 10-base
/// Useful as R has no 64bit integer type
pub fn usize_to_robj_str(us: usize) -> Robj {
    format!("{us}").into()
}
