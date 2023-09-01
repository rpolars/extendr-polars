// functions for arrow2 polars handling

use crate::odi;
use crate::rptr;
use extendr_api::prelude::Result as EResult;
use extendr_api::prelude::Robj;
use extendr_api::prelude::*;
use polars::prelude as pl;
use polars_core::utils::arrow::ffi;

pub fn to_rpolars_dataframe(df: pl::DataFrame) -> EResult<Robj> {
    // get stream + stream ptr from r-polars
    let robj_str = R!("polars:::new_arrow_stream()")?;

    // safety: polars:::new_arrow_stream produces a robj_str pointer to a valid stream
    unsafe { export_df_as_stream(df, &robj_str)? }

    // request r-polars to consume stream and produce an Robj of an r-polars DataFrame.
    let robj_df: Robj = R!("polars:::arrow_stream_to_df({{robj_str}}) |> polars:::unwrap('exporting DataFrame to r-polars')")?;
    // Transmuting naively robj_df to a rust-polars DataFrame outside r-polars lib is Undefined Behavior.

    Ok(robj_df)
}

// safety: requires a valid array stream pointer, robj_str_ref
unsafe fn export_df_as_stream(df: pl::DataFrame, robj_str_ref: &Robj) -> EResult<()> {
    let stream_ptr = rptr::robj_str_ptr_to_usize(robj_str_ref)? as *mut ffi::ArrowArrayStream;
    let schema = df.schema().to_arrow();
    let data_type = pl::ArrowDataType::Struct(schema.fields);
    let field = pl::ArrowField::new("", data_type, false);
    let iter_boxed = Box::new(odi::OwnedDataFrameIterator::new(df));
    unsafe { *stream_ptr = ffi::export_iterator(iter_boxed, field) };
    Ok(())
}
