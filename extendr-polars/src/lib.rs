use arrow::datatypes::DataType as ADataType;
use arrow2::ffi;
use extendr_api::prelude::*;
use polars::prelude as pl;
use polars_core::utils::arrow;
//#[cfg(feature = "lazy")]
//use {polars_lazy::frame::LazyFrame, polars_plan::logical_plan::LogicalPlan};

// #[repr(transparent)]
// #[derive(Debug, Clone)]
// /// A wrapper around a [`Series`] that can be converted to and from python with `pyo3`.
// pub struct RSeries(pub pl::Series);

#[repr(transparent)]
#[derive(Debug, Clone)]
/// A wrapper around a [`DataFrame`] that can be converted to and from python with `pyo3`.
pub struct WrapDataFrame(pub pl::DataFrame);

// #[cfg(feature = "lazy")]
// #[repr(transparent)]
// #[derive(Clone)]
/// A wrapper around a [`DataFrame`] that can be converted to and from python with `pyo3`.
/// # Warning
/// If the [`LazyFrame`] contains in memory data,
/// such as a [`DataFrame`] this will be serialized/deserialized.
///
/// It is recommended to only have `LazyFrame`s that scan data
/// from disk
//pub struct RLazyFrame(pub LazyFrame);

impl From<WrapDataFrame> for pl::DataFrame {
    fn from(value: WrapDataFrame) -> Self {
        value.0
    }
}

pub struct OwnedDataFrameIterator {
    columns: Vec<polars::series::Series>,
    data_type: arrow::datatypes::DataType,
    idx: usize,
    n_chunks: usize,
}

impl OwnedDataFrameIterator {
    fn new(df: polars::frame::DataFrame) -> Self {
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

#[extendr]
impl WrapDataFrame {
    pub fn export_stream(&self) -> Result<Robj> {
        let schema = self.0.schema().to_arrow();
        let data_type = pl::ArrowDataType::Struct(schema.fields);
        let field = pl::ArrowField::new("", data_type, false);

        let iter_boxed = Box::new(OwnedDataFrameIterator::new(self.0.clone()));
        let mut stream = ffi::export_iterator(iter_boxed, field);
        //let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();
        let x = &mut stream as *mut ffi::ArrowArrayStream;
        let y = x as usize;
        dbg!(y);
        let s_ptr = format!("{:x}", y);
        dbg!(&s_ptr);
        let rx = s_ptr.into_robj();
        let res_robj = R!("browser();2+2;x = {{rx}} ; polars:::import_arrow_array_stream(x)");

        if res_robj.is_err() {
            // TODO some clean up, release ownership of ArrowArrayStream some how. //
            rprintln!("extendr_polars failed to export");
        }

        res_robj
    }
}

// impl From<RSeries> for pl::Series {
//     fn from(value: RSeries) -> Self {
//         value.0
//     }
// }

// #[extendr]
// impl RSeries {
//     fn debug(&self) -> String {
//         let x = format!("{:?}", self);
//         x
//     }
// }

// fn s_to_robj(s_ref: &RSeries) -> Robj {
//     let s = s_ref.clone();
//     let ptr_string = format!("{:p}", &s);
//     std::mem::forget(s); //release ownership
//     let robj = R!("{polars:::series_from_ptr_string({ptr_string})}").expect("shit hit the fan");

//     robj
// }

// #[cfg(feature = "lazy")]
// impl From<RLazyFrame> for LazyFrame {
//     fn from(value: LazyFrame) -> Self {
//         value.0
//     }
// }

// impl AsRef<pl::Series> for RSeries {
//     fn as_ref(&self) -> &pl::Series {
//         &self.0
//     }
// }

// impl AsRef<pl::DataFrame> for WrapDataFrame {
//     fn as_ref(&self) -> &pl::DataFrame {
//         &self.0
//     }
// }

// #[cfg(feature = "lazy")]
// impl AsRef<pl::LazyFrame> for RLazyFrame {
//     fn as_ref(&self) -> &pl::LazyFrame {
//         &self.0
//     }
// }

// pub fn try_robj_to_series(ob: Robj) -> extendr_api::Result<RSeries> {
//     let x = if ob.inherits("Series") {
//         Ok(unsafe { &mut *ob.external_ptr_addr::<RSeries>() }.clone())
//     } else {
//         Err("extendr-polars: invalid input, is not a RSeries")
//     }?;
//     Ok(x)
// }

// pub fn try_robj_to_dataframe(ob: Robj) -> extendr_api::Result<WrapDataFrame> {
//     let x = if ob.inherits("Series") {
//         Ok(unsafe { &mut *ob.external_ptr_addr::<WrapDataFrame>() }.clone())
//     } else {
//         Err("extendr-polars: invalid inputWrapDataFrame")
//     }?;
//     Ok(x)
// }

// #[cfg(feature = "lazy")]
// pub fn try_robj_to_lazyframe(ob: Robj) -> extendr_api::Result<RLazyFrame> {
//     let x = if ob.inherits("Series") {
//         Ok(unsafe { &mut *ob.external_ptr_addr::<RLazyFrame>() }.clone())
//     } else {
//         Err("extendr-polars: invalid inputRLazyFrame")
//     }?;
//     Ok(x)
// }

extendr_module! {
    mod extendr_polars;
    impl WrapDataFrame;
}
