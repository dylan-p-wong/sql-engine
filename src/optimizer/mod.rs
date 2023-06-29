use std::fmt::Error;

use crate::planner::Plan;

pub struct Optimizer {}

impl Optimizer {
    pub fn new() -> Optimizer {
        Optimizer {}
    }

    pub fn optimize(&self, plan : Plan) -> Result<Plan, Error> {
        println!("Optimizing...");
        return Ok(plan);
    }
}
