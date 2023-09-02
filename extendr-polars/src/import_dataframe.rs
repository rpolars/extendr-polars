use extendr_api::prelude::*;
use polars::prelude as pl;
use polars_core::prelude::PolarsResult;
use polars_core::utils::arrow::ffi;
use std::result::Result;

// let r-polars export DataFrame robj_df into a rust-polars DataFrame
pub fn rpolars_to_rust_dataframe(robj_df: Robj) -> pl::PolarsResult<pl::DataFrame> {
    // make new stream
    let stream_str_ptr = new_arrow_stream();

    // pass ptr to r-polars and let it export robj_df into it
    let stream_str_ptr = R!(
        "polars:::export_df_to_arrow_stream({{robj_df}}, {{stream_str_ptr}}) |> polars:::unwrap()"
    )
    .map_err(|err| pl::PolarsError::ComputeError(err.to_string().into()))?;

    // consume arrow stream to produce a rust-polars DataFrame
    import_arrow_stream_to_rust_polars_df(stream_str_ptr)
}

// r-polars as consumer 1: create a new stream and wrap pointer in Robj as str.
fn new_arrow_stream() -> Robj {
    let aas = Box::new(ffi::ArrowArrayStream::empty());
    let x = Box::leak(aas); // leak box to make lifetime static
    let x = x as *mut ffi::ArrowArrayStream;
    crate::rptr::usize_to_robj_str(x as usize)
}

fn import_arrow_stream_to_rust_polars_df(robj_str: Robj) -> PolarsResult<pl::DataFrame> {
    let s = arrow_stream_to_series_internal(robj_str)
        .map_err(|err| pl::PolarsError::ComputeError(err.to_string().into()))?;
    let ca = s.struct_()?;
    let df: pl::DataFrame = ca.clone().into();
    Ok(df)
}

// r-polars as consumer 2: recieve to pointer to own stream, which producer has exported to. Consume it. Return Series.
fn arrow_stream_to_series_internal(
    robj_str: Robj,
) -> Result<pl::Series, Box<dyn std::error::Error>> {
    // reclaim ownership of leaked box, and then drop/release it when consumed.
    let us = crate::rptr::robj_str_ptr_to_usize(&robj_str)?;
    let boxed_stream = unsafe { Box::from_raw(us as *mut ffi::ArrowArrayStream) };

    //consume stream and produce a r-polars Series return as Robj
    let s = consume_arrow_stream_to_series(boxed_stream)?;
    Ok(s)
}

// implementation of consuming stream to Series. Stream is drop/released hereafter.
fn consume_arrow_stream_to_series(
    boxed_stream: Box<ffi::ArrowArrayStream>,
) -> Result<pl::Series, pl::PolarsError> {
    let mut iter = unsafe { ffi::ArrowArrayStreamReader::try_new(boxed_stream) }?;

    //import first array into pl::Series
    let mut s = if let Some(array_res) = unsafe { iter.next() } {
        let array = array_res?;
        let series_res: pl::PolarsResult<pl::Series> =
            std::convert::TryFrom::try_from(("df", array));
        let series = series_res?;
        series
    } else {
        Err(pl::PolarsError::InvalidOperation(
            "\
        When consuming arrow array stream. \
        Arrow array stream was empty. \
        Probably producer did not export to stream. When\
        "
            .into(),
        ))?;
        unreachable!();
    };

    // append any other arrays to Series
    while let Some(array_res) = unsafe { iter.next() } {
        let array = array_res?;
        let series_res: pl::PolarsResult<pl::Series> =
            std::convert::TryFrom::try_from(("df", array));
        let series = series_res?;
        s.append(&series)?;
    }
    Ok(s)
}
