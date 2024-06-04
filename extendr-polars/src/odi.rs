// OwnedDataFrameIterator written by @paleolimbot / nanoarrow in PR for pola-rs/r-polars

use polars::error::PolarsError;
use polars_core::utils::arrow;
use polars_core::utils::arrow::datatypes::ArrowDataType as ADataType;
use polars_core::utils::arrow::array::Array;

pub struct OwnedDataFrameIterator {
    columns: Vec<polars::series::Series>,
    data_type: ADataType,
    idx: usize,
    n_chunks: usize,
}

impl OwnedDataFrameIterator {
    pub fn new(df: polars::frame::DataFrame) -> Self {
        let schema = df.schema().to_arrow(true);
        let data_type = ADataType::Struct(schema.fields);
        let vs = df.get_columns().to_vec();
        Self {
            columns: vs,
            data_type,
            idx: 0,
            n_chunks: df.n_chunks(),
        }
    }
}

impl Iterator for OwnedDataFrameIterator {
    type Item = std::result::Result<Box<dyn arrow::array::Array>, PolarsError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.n_chunks {
            None
        } else {
            // create a batch of the columns with the same chunk no.
            let batch_cols: Vec<Box<dyn Array>> = self.columns.iter().map(|s| s.to_arrow(self.idx, true)).collect();
            self.idx += 1;

            let array = arrow::array::StructArray::new(
                self.data_type.clone(),
                batch_cols,
                std::option::Option::None,
            );
            Some(std::result::Result::Ok(Box::new(array)))
        }
    }
}
