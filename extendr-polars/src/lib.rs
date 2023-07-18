use extendr_api::prelude::*;
use polars::prelude as pl;
use polars_core::utils::arrow;
use polars_core::utils::arrow::datatypes::DataType as ADataType;
use polars_core::utils::arrow::ffi;
//#[cfg(feature = "lazy")]
//use {polars_lazy::frame::LazyFrame, polars_plan::logical_plan::LogicalPlan};

// #[repr(transparent)]
// #[derive(Debug, Clone)]
// /// A wrapper around a [`Series`] that can be converted to and from python with `pyo3`.
// pub struct RSeries(pub pl::Series);
//keep error simple to interface with other libs
pub fn robj_str_ptr_to_usize(robj: &Robj) -> std::result::Result<usize, String> {
    let str: &str = robj.as_str().ok_or("robj str ptr not a str".to_string())?;
    let us: usize = str.parse().map_err(|err| format!("parse error : {err}"))?;
    Ok(us)
}

pub fn usize_to_robj_str(us: usize) -> Robj {
    format!("{us}").into()
}

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
    data_type: ADataType,
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
        let df_clone = self.0.clone();

        let schema = df_clone.schema().to_arrow();
        let data_type = pl::ArrowDataType::Struct(schema.fields);
        let field = pl::ArrowField::new("", data_type, false);

        let iter_boxed = Box::new(OwnedDataFrameIterator::new(df_clone));
        let mut stream = Box::new(ffi::ArrowArrayStream::empty());
        *stream = ffi::export_iterator(iter_boxed, field);

        //let stream_out_ptr_addr: usize = stream_ptr.parse().unwrap();
        let x = Box::leak(stream) as *mut ffi::ArrowArrayStream;
        //let x = &mut *stream as *mut ffi::ArrowArrayStream;
        // release ownship as it is transferred to polars::: below

        let y = x as usize;
        dbg!(y);
        let s_ptr = format!("{}", y);
        dbg!(&s_ptr);
        rprintln!("in hex it is: {:x}", y);
        let rx = s_ptr.into_robj();

        let robj = R!("polars:::import_arrow_array_stream({{rx}}) |> (\\(x) if(is.null(x$err)) x$ok else stop(x$err))()");

        robj
    }

    pub fn make_dataframe(&self) -> Result<Robj> {
        // get stream + stream ptr from r-polars
        let robj_str = R!("polars:::new_arrow_stream()")?;
        let stream_ptr = robj_str_ptr_to_usize(&robj_str)? as *mut ffi::ArrowArrayStream;

        // export to stream
        let df_clone = self.0.clone();
        let schema = df_clone.schema().to_arrow();
        let data_type = pl::ArrowDataType::Struct(schema.fields);
        let field = pl::ArrowField::new("", data_type, false);
        let iter_boxed = Box::new(OwnedDataFrameIterator::new(df_clone));
        unsafe { *stream_ptr = ffi::export_iterator(iter_boxed, field) };

        // request r-polars to consume stream and produce an Robj of an r-polars DataFrame.
        // Only interact with this DataFrame via r-polars. Transmuting naively to rust-polars DataFrame is Undefined Behavior.
        // Use dedicated method to convert back.
        let robj_df: Robj = R!("polars:::arrow_stream_to_df({{robj_str}})")?;

        Ok(robj_df)
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
