use std::{cell::RefCell, collections::{HashMap, HashSet}, rc::Rc};
use serde::Serialize;
use mlua::prelude::*;

#[derive(Debug, Serialize)]
pub enum Action {
    Save {
        name: String,
        value: Option<f32>,
    },
    WriteParameter {
        parameter_name: String,
        new_value: Option<f32>,
    },
    CreateCase {
        title: String,
        description: String,
    },
}

pub fn run_lua_with_data(code: &str, data: HashMap<String, Option<f32>>) -> LuaResult<Vec<Action>> {
    let actions: Rc<RefCell<Vec<Action>>> = Rc::new(RefCell::new(Vec::new()));
    let lua = Lua::new();

    let globals = lua.globals();
    for (key, val) in data.into_iter() {
        match val {
            Some(f) => globals.set(key, f)?,
            None => globals.set(key, mlua::Value::Nil)?,
        }
    }

    attach_action_functions(&lua, actions.clone())?;

    lua.load(code).exec()?;

    let collected = drain_collected_actions(actions);
    Ok(keep_only_last_of_same_name(collected))
}

pub fn run_lua_with_data_daily(
    code: &str,
    data: HashMap<String, Vec<Option<f32>>>,
) -> LuaResult<Vec<Action>> {
    let actions: Rc<RefCell<Vec<Action>>> = Rc::new(RefCell::new(Vec::new()));
    let lua = Lua::new();
    let globals = lua.globals();

    for (key, series) in data.into_iter() {
        let all_none = series.iter().all(|v| v.is_none());
        if all_none {
            globals.set(key, mlua::Value::Nil)?;
        } else {
            let tbl = lua.create_table()?;
            for (i, val) in series.into_iter().enumerate() {
                let idx = (i + 1) as i64; // Lua tables are 1-indexed
                match val {
                    Some(f) => tbl.set(idx, f)?,
                    None => tbl.set(idx, mlua::Value::Nil)?,
                }
            }
            globals.set(key, tbl)?;
        }
    }

    attach_action_functions(&lua, actions.clone())?;

    lua.load(code).exec()?;

    let collected = drain_collected_actions(actions);
    Ok(keep_only_last_of_same_name(collected))
}

fn attach_action_functions(lua: &Lua, actions: Rc<RefCell<Vec<Action>>>) -> LuaResult<()> {
    let globals = lua.globals();

    {
        let actions_clone = actions.clone();
        let save = lua.create_function(move |_, (name, value): (String, Option<f32>)| {
            actions_clone
                .borrow_mut()
                .push(Action::Save { name, value });
            Ok(())
        })?;
        globals.set("save", save)?;
    }

    {
        let actions_clone = actions.clone();
        let write_parameter = lua.create_function(
            move |_, (parameter_name, new_value): (String, Option<f32>)| {
                actions_clone.borrow_mut().push(Action::WriteParameter {
                    parameter_name,
                    new_value,
                });
                Ok(())
            },
        )?;
        globals.set("write_parameter", write_parameter)?;
    }

    {
        let actions_clone = actions;
        let create_case =
            lua.create_function(move |_, (title, description): (String, String)| {
                actions_clone
                    .borrow_mut()
                    .push(Action::CreateCase { title, description });
                Ok(())
            })?;
        globals.set("create_case", create_case)?;
    }

    Ok(())
}

fn drain_collected_actions(actions: Rc<RefCell<Vec<Action>>>) -> Vec<Action> {
    let mut collected: Vec<Action> = Vec::new();
    {
        let mut a = actions.borrow_mut();
        collected.extend(a.drain(..));
    }
    collected
}

fn keep_only_last_of_same_name(actions: Vec<Action>) -> Vec<Action> {
    let mut seen_write_parameter: HashSet<String> = HashSet::new();
    let mut seen_save: HashSet<String> = HashSet::new();
    let mut result: Vec<Action> = Vec::new();
    for action in actions.into_iter().rev() {
        match &action {
            Action::WriteParameter { parameter_name, .. } => {
                if !seen_write_parameter.contains(parameter_name) {
                    seen_write_parameter.insert(parameter_name.clone());
                    result.push(action);
                }
            }
            Action::Save { name, .. } => {
                if !seen_save.contains(name) {
                    seen_save.insert(name.clone());
                    result.push(action);
                }
            }
            _ => {
                result.push(action);
            }
        }
    }
    result.reverse();
    result
}
