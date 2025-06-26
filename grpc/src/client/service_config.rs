/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
use std::{any::Any, error::Error, sync::Arc};

/// An in-memory representation of a service config, usually provided to gRPC as
/// a JSON object.
#[derive(Debug, Default, Clone)]
pub struct ServiceConfig;

/// A convenience wrapper for an LB policy's configuration object.
#[derive(Debug)]
#[derive(Clone)]

pub struct LbConfig {
    config: Arc<dyn Any + Send + Sync>,
}

impl LbConfig {
    /// Create a new LbConfig wrapper containing the provided config.
    pub fn new<T: 'static + Send + Sync>(config: T) -> Self {
        LbConfig {
            config: Arc::new(config),
        }
    }

    /// Convenience method to extract the LB policy's configuration object.
    pub fn convert_to<T: 'static + Send + Sync>(
        &self,
    ) -> Result<Arc<T>, Box<dyn Error + Send + Sync>> {
        println!("TypeId in LbConfig: {:?}", self.config.type_id());
        
        match self.config.clone().downcast::<T>() {
            Ok(c) => Ok(c),
            Err(e) => Err("failed to downcast to config type".into()),
        }
    }

   
}
