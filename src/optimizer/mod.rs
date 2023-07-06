use crate::{planner::Plan, types::error::Error};

pub struct Optimizer {}

impl Optimizer {
    pub fn new() -> Optimizer {
        Optimizer {}
    }

    pub fn optimize(&self, plan: Plan) -> Result<Plan, Error> {
        Ok(plan)
    }
}
