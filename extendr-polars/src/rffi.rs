// functions for arrow2 polars handling

use crate::odi;
use crate::rptr;
use extendr_api::prelude::Result as EResult;
use extendr_api::prelude::Robj;
use polars::prelude as pl;
use polars_core::utils::arrow::ffi;

pub unsafe fn export_df_as_stream(df: pl::DataFrame, robj_str_ref: &Robj) -> EResult<()> {
    let stream_ptr = rptr::robj_str_ptr_to_usize(robj_str_ref)? as *mut ffi::ArrowArrayStream;
    let schema = df.schema().to_arrow();
    let data_type = pl::ArrowDataType::Struct(schema.fields);
    let field = pl::ArrowField::new("", data_type, false);
    let iter_boxed = Box::new(odi::OwnedDataFrameIterator::new(df));
    unsafe { *stream_ptr = ffi::export_iterator(iter_boxed, field) };
    Ok(())
}
