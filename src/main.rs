use std::io::{self, Write};
use MetaCommandResult::*;
use StatementType::*;
use PrepareResult::*;
use std::fmt;

mod bufferPool;
use bufferPool::BufferPool;
mod page;
use page::Pager;

enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
    NotFound,
}

#[derive(PartialEq)]
enum PrepareResult {
    Prepare_success,
    Prepare_unrecognized_statement
}

enum StatementType {
    Insert,
    Select,
    Delete,
}

impl fmt::Display for StatementType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatementType::Insert => write!(f, "INSERT"),
            StatementType::Select => write!(f, "SELECT"),
            StatementType::Delete => write!(f, "DELETE"),
        }
    }
}

struct InputBuffer {
    buffer: [u32; 1024],  
    length: usize, 
}

impl InputBuffer {
    fn new() -> Self {
        InputBuffer { buffer: [0; 1024], length: 0 }
    }

    fn add(&mut self, value: u32) {
        if self.length < self.buffer.len() {
            self.buffer[self.length] = value;
            self.length += 1;
        } else {
            panic!("Buffer overflow");
        }
    }

    fn get(&mut self) -> Option<String> {
        if self.length == 0 {
            None
        } else {
            Some(self.to_string())
        }
    }

    fn to_string(&self) -> String {
        self.buffer[..self.length]
            .iter()
            .filter_map(|&code| char::from_u32(code))
            .collect()
    }

    fn clear_buffer(&mut self) {
        self.buffer = [0; 1024];
        self.length = 0;
    }

    fn do_meta_command(&self) -> MetaCommandResult {
        let input_str = self.to_string();
        if input_str.is_empty() {
            NotFound
        } else if input_str == ".exit" {
            Success
        } else {
            UnrecognizedCommand
        }
    }
}

struct Statement {
    _type: StatementType,
    buffer: [u32; 1024],
    length: usize,
}

impl Statement {
    fn new() -> Self {
        Statement { _type: Select, buffer: [0; 1024], length: 0 }
    }
    

    fn prepare_statement(&mut self, input_buffer: &mut InputBuffer) -> PrepareResult {
        match input_buffer.get() {
            None => Prepare_unrecognized_statement,
            Some(input) => {
                match input.as_str() {
                    s if s.starts_with("insert") => {
                        self.buffer = input_buffer.buffer;
                        self.length = input_buffer.length;
                        self._type = Insert;
                        Prepare_success
                    }, 
                    s if s.starts_with("select") => {
                        self.buffer = input_buffer.buffer;
                        self.length = input_buffer.length;
                        self._type = Select;
                        Prepare_success
                    },
                    s if s.starts_with("delete") => {
                        self.buffer = input_buffer.buffer;
                        self.length = input_buffer.length;
                        self._type = Delete;
                        Prepare_success
                    }
                    _ => {
                        Prepare_unrecognized_statement
                    }
                }
            }
        }
    }

    fn execute_statement(&self) {
        match self._type {
            Insert => println!("Do insert"),
            Select => println!("Do select"),
            Delete => println!("Do delete"),
        }
    }

    fn clear_buffer(&mut self) {
        self.buffer = [0; 1024];
        self.length = 0;
    }

}

fn print_prompt() {
    print!("db > ");
}

fn read_input(input_buffer: &mut InputBuffer) {
    let mut input = String::new();
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut input).unwrap();
    
    let input = input.trim();
    
    for ch in input.chars() {
        let code_point = ch as u32;
        input_buffer.add(code_point);
    }
    
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input_buffer = InputBuffer::new();
    let mut statement = Statement::new();
    
    loop {
        print_prompt();
        read_input(&mut input_buffer);

        match input_buffer.get(){
            None => continue,
            Some(input) => {
                if input.contains(".") {
                    let meta_command_result = input_buffer.do_meta_command();
                    match meta_command_result {
                        Success => {
                            if input == ".exit" {
                                return Ok(())
                            }
                        },
                        UnrecognizedCommand => {
                            println!("Unrecognized command {}", input);
                        },
                        NotFound => {
                            println!("Empty Input");
                        }
                    }
                } else {
                    let prepare_result = statement.prepare_statement(&mut input_buffer);
                    if prepare_result == Prepare_unrecognized_statement {
                        println!("unrecognized keyword");
                    } else {
                        println!("{}", statement._type);
                        statement.execute_statement();
                    }
                }
            }
        }
        input_buffer.clear_buffer();
        statement.clear_buffer();
    }
}
