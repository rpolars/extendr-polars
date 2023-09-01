// OwnedDataFrameIterator written by @paleolimbot / nanoarrow in PR for r-polars

use polars_core::utils::arrow;
use polars_core::utils::arrow::datatypes::DataType as ADataType;

pub struct OwnedDataFrameIterator {
    columns: Vec<polars::series::Series>,
    data_type: ADataType,
    idx: usize,
    n_chunks: usize,
}

impl OwnedDataFrameIterator {
    pub fn new(df: polars::frame::DataFrame) -> Self {
        let schema = df.schema().to_arrow();
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
    type Item = std::result::Result<Box<dyn arrow::array::Array>, arrow::error::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.n_chunks {
            None
        } else {
            // create a batch of the columns with the same chunk no.
            let batch_cols = self.columns.iter().map(|s| s.to_arrow(self.idx)).collect();
            self.idx += 1;

            let chunk = polars::frame::ArrowChunk::new(batch_cols);
            let array = arrow::array::StructArray::new(
                self.data_type.clone(),
                chunk.into_arrays(),
                std::option::Option::None,
            );
            Some(std::result::Result::Ok(Box::new(array)))
        }
    }
}
