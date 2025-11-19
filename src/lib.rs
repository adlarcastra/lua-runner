use std::{cell::RefCell, collections::{HashMap, HashSet}, rc::Rc, sync::{atomic::{AtomicU64, Ordering}, Arc}};
use serde::Serialize;
use mlua::prelude::*;
use mlua::HookTriggers;
use mlua::VmState;
use mlua::Variadic;

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


const INSTRUCTION_STEP: u32 = 1_000;
const MAX_HOOK_CALLS: u64 = 1_000;
const MEMORY_LIMIT_BYTES: usize = 10 * 1024 * 1024;

fn configure_lua_limits(lua: &Lua) -> LuaResult<()> {
    lua.set_memory_limit(MEMORY_LIMIT_BYTES)?;

    let hook_calls = Arc::new(AtomicU64::new(0));
    let hook_calls_clone = hook_calls.clone();

    lua.set_hook(
        HookTriggers::new().every_nth_instruction(INSTRUCTION_STEP),
        move |_lua, _debug| {
            let calls = hook_calls_clone.fetch_add(1, Ordering::Relaxed) + 1;
            if calls > MAX_HOOK_CALLS {
                return Err(LuaError::RuntimeError(
                    "script exceeded instruction limit (possible infinite loop)".into(),
                ));
            }
            Ok(VmState::Continue)
        },
    )?;

    Ok(())
}


pub fn run_lua_with_data(
    code: &str,
    data: std::collections::HashMap<String, Option<f32>>,
) -> LuaResult<(Vec<Action>, String)> {
    let actions: Rc<RefCell<Vec<Action>>> = Rc::new(RefCell::new(Vec::new()));
    let lua = Lua::new();

    configure_lua_limits(&lua)?;

    let stdout_buf: Rc<RefCell<String>> = Rc::new(RefCell::new(String::new()));
    let stdout_buf_clone = stdout_buf.clone();

    let print_fn = lua.create_function(move |_, args: Variadic<String>| {
        let mut s = String::new();
        for (i, part) in args.iter().enumerate() {
            if i > 0 {
                s.push('\t');
            }
            s.push_str(part);
        }
        s.push('\n');
        stdout_buf_clone.borrow_mut().push_str(&s);
        Ok(())
    })?;
    lua.globals().set("print", print_fn)?;

    if let Ok(io_table) = lua.globals().get::<mlua::Table>("io") {
        let stdout_buf_for_io = stdout_buf.clone();
        let io_write = lua.create_function(move |_, args: Variadic<String>| {
            for part in args.iter() {
                stdout_buf_for_io.borrow_mut().push_str(part);
            }
            Ok(())
        })?;
        let _ = io_table.set("write", io_write);
        if let Ok(stdout_obj) = io_table.get::<mlua::Value>("stdout") {
            if let mlua::Value::Table(tbl) = stdout_obj {
                let stdout_buf_for_stdout = stdout_buf.clone();
                let write_fn = lua.create_function(move |_, args: Variadic<String>| {
                    for part in args.iter() {
                        stdout_buf_for_stdout.borrow_mut().push_str(part);
                    }
                    Ok(())
                })?;
                let _ = tbl.set("write", write_fn);
            }
        }
    }

    let globals = lua.globals();
    for (key, val) in data.into_iter() {
        match val {
            Some(f) => globals.set(key, f)?,
            None => globals.set(key, mlua::Value::Nil)?,
        }
    }

    attach_action_functions(&lua, actions.clone())?;

    match lua.load(code).exec() {
        Ok(()) => {
            let collected = drain_collected_actions(actions);
            let actions = keep_only_last_of_same_name(collected);
            let output = stdout_buf.borrow().clone();
            Ok((actions, output))
        }
        Err(e) => Err(e),
    }
}

pub fn run_lua_with_data_daily(
    code: &str,
    data: HashMap<String, Vec<Option<f32>>>,
) -> LuaResult<(Vec<Action>, String)> {
    let actions: Rc<RefCell<Vec<Action>>> = Rc::new(RefCell::new(Vec::new()));
    let lua = Lua::new();

    configure_lua_limits(&lua)?;

    let stdout_buf: Rc<RefCell<String>> = Rc::new(RefCell::new(String::new()));
    let stdout_buf_clone = stdout_buf.clone();

    let print_fn = lua.create_function(move |_, args: Variadic<String>| {
        let mut s = String::new();
        for (i, part) in args.iter().enumerate() {
            if i > 0 {
                s.push('\t');
            }
            s.push_str(part);
        }
        s.push('\n');
        stdout_buf_clone.borrow_mut().push_str(&s);
        Ok(())
    })?;
    lua.globals().set("print", print_fn)?;

    if let Ok(io_table) = lua.globals().get::<mlua::Table>("io") {
        let stdout_buf_for_io = stdout_buf.clone();
        let io_write = lua.create_function(move |_, args: Variadic<String>| {
            for part in args.iter() {
                stdout_buf_for_io.borrow_mut().push_str(part);
            }
            Ok(())
        })?;
        let _ = io_table.set("write", io_write);

        if let Ok(stdout_obj) = io_table.get::<mlua::Value>("stdout") {
            if let mlua::Value::Table(tbl) = stdout_obj {
                let stdout_buf_for_stdout = stdout_buf.clone();
                let write_fn = lua.create_function(move |_, args: Variadic<String>| {
                    for part in args.iter() {
                        stdout_buf_for_stdout.borrow_mut().push_str(part);
                    }
                    Ok(())
                })?;
                let _ = tbl.set("write", write_fn);
            }
        }
    }

    let globals = lua.globals();
    for (key, series) in data.into_iter() {
        let all_none = series.iter().all(|v| v.is_none());
        if all_none {
            globals.set(key, mlua::Value::Nil)?;
        } else {
            let tbl = lua.create_table()?;
            for (i, val) in series.into_iter().enumerate() {
                let idx = (i + 1) as i64;
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
    let actions = keep_only_last_of_same_name(collected);
    let output = stdout_buf.borrow().clone();
    Ok((actions, output))
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn assert_actions_eq(actual: Vec<Action>, expected: Vec<Action>) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "action length mismatch: got {}, expected {}.\nGot:  {:#?}\nWant: {:#?}",
            actual.len(),
            expected.len(),
            actual,
            expected
        );

        for (i, (a, e)) in actual.into_iter().zip(expected.into_iter()).enumerate() {
            match (a, e) {
                (
                    Action::Save { name: an, value: av },
                    Action::Save { name: en, value: ev },
                ) => {
                    assert_eq!(
                        an, en,
                        "mismatch at index {}: Save.name: got {}, expected {}",
                        i, an, en
                    );
                    assert!(
                        (av.is_none() && ev.is_none()) || (av.is_some() && ev.is_some() && av == ev),
                        "mismatch at index {}: Save.value: got {:?}, expected {:?}",
                        i,
                        av,
                        ev
                    );
                }

                (
                    Action::WriteParameter {
                        parameter_name: ap,
                        new_value: av,
                    },
                    Action::WriteParameter {
                        parameter_name: ep,
                        new_value: ev,
                    },
                ) => {
                    assert_eq!(
                        ap, ep,
                        "mismatch at index {}: WriteParameter.parameter_name: got {}, expected {}",
                        i, ap, ep
                    );
                    assert!(
                        (av.is_none() && ev.is_none()) || (av.is_some() && ev.is_some() && av == ev),
                        "mismatch at index {}: WriteParameter.new_value: got {:?}, expected {:?}",
                        i,
                        av,
                        ev
                    );
                }

                (
                    Action::CreateCase {
                        title: at,
                        description: ad,
                    },
                    Action::CreateCase {
                        title: et,
                        description: ed,
                    },
                ) => {
                    assert_eq!(
                        at, et,
                        "mismatch at index {}: CreateCase.title: got {}, expected {}",
                        i, at, et
                    );
                    assert_eq!(
                        ad, ed,
                        "mismatch at index {}: CreateCase.description: got {}, expected {}",
                        i, ad, ed
                    );
                }

                (got_variant, want_variant) => {
                    panic!(
                        "mismatch at index {}: variant mismatch.\n got:  {:#?}\n want: {:#?}",
                        i, got_variant, want_variant
                    );
                }
            }
        }
    }

    #[test]
    fn test_run_lua_with_data_actions_and_last_only() {
        let code = r#"
            save("a", 1.0)
            save("a", 2.0)
            write_parameter("p", 3.0)
            write_parameter("p", 4.0)
            create_case("title-1", "desc-1")
        "#;

        let data: HashMap<String, Option<f32>> = HashMap::new();
        let res = run_lua_with_data(code, data).expect("should run lua");

        let expected = vec![
            Action::Save {
                name: "a".to_string(),
                value: Some(2.0),
            },
            Action::WriteParameter {
                parameter_name: "p".to_string(),
                new_value: Some(4.0),
            },
            Action::CreateCase {
                title: "title-1".to_string(),
                description: "desc-1".to_string(),
            },
        ];

        assert_actions_eq(res.0, expected);
    }

    #[test]
    fn test_run_lua_with_data_daily_tables_and_nil() {
        let mut daily: HashMap<String, Vec<Option<f32>>> = HashMap::new();
        daily.insert("all_none".to_string(), vec![None, None, None]);
        daily.insert("series".to_string(), vec![None, Some(1.5), None]);

        let code = r#"
            if all_none == nil then
                save("all_none_is_nil", nil)
            else
                save("all_none_is_not_nil", nil)
            end

            if type(series) == "table" then
                save("series_first", series[1])
                save("series_second", series[2])
            else
                save("series_not_table", nil)
            end
        "#;

        let res = run_lua_with_data_daily(code, daily).expect("should run daily lua");

        let expected = vec![
            Action::Save {
                name: "all_none_is_nil".to_string(),
                value: None,
            },
            Action::Save {
                name: "series_first".to_string(),
                value: None,
            },
            Action::Save {
                name: "series_second".to_string(),
                value: Some(1.5),
            },
        ];

        assert_actions_eq(res.0, expected);
    }

    #[test]
    fn test_run_lua_with_data_infinite_loop_is_aborted_by_hook() {
        let code = r#"
            while true do
            end
        "#;

        let data: HashMap<String, Option<f32>> = HashMap::new();
        let err = run_lua_with_data(code, data).unwrap_err();

        let s = format!("{}", err);
        assert!(
            s.contains("script exceeded instruction limit"),
            "unexpected error message: {}",
            s
        );
    }

    #[test]
    fn test_run_lua_with_data_captures_stdout_and_actions() {
        let code = r#"
            print("hello", 123)
            io.write("no-newline")
            print("after")
            save("x", 5.5)
        "#;

        let data: HashMap<String, Option<f32>> = HashMap::new();
        let (res, stdout) = run_lua_with_data(code, data).expect("should run lua and return (actions, stdout)");

        let expected = vec![
            Action::Save {
                name: "x".to_string(),
                value: Some(5.5),
            },
        ];
        assert_actions_eq(res, expected);

        let expected_stdout = "hello\t123\nno-newlineafter\n";
        assert_eq!(
            stdout, expected_stdout,
            "captured stdout did not match.\nGot:\n{:?}\nWant:\n{:?}",
            stdout, expected_stdout
        );
    }
}
