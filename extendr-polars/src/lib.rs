use extendr_api::prelude::*;
use polars::prelude as pl;
pub mod odi;
pub mod rffi;
pub mod rptr;

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct WrapDataFrame(pub pl::DataFrame);

impl From<WrapDataFrame> for pl::DataFrame {
    fn from(value: WrapDataFrame) -> Self {
        value.0
    }
}

#[extendr]
impl WrapDataFrame {
    pub fn make_dataframe(&self) -> Result<Robj> {
        // get stream + stream ptr from r-polars
        let robj_str = R!("polars:::new_arrow_stream()")?;

        // export to stream
        unsafe { rffi::export_df_as_stream(self.0.clone(), &robj_str)? }

        // request r-polars to consume stream and produce an Robj of an r-polars DataFrame.
        let robj_df: Robj = R!("polars:::arrow_stream_to_df({{robj_str}}) |> polars:::unwrap('exporting DataFrame to r-polars')")?;
        // Transmuting naively robj_df to a rust-polars DataFrame outside r-polars lib is Undefined Behavior.

        Ok(robj_df)
    }
}
