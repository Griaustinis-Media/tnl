require_relative 'lexer'
require_relative 'ast/ast'
require_relative 'parser'

module Tsang
  VERSION = '0.1.0'
  
  class << self
    def parse(sql_text)
      tokens = Lexer.new(sql_text).tokenize
      Parser.new(tokens).parse
    end
    
    def tokenize(sql_text)
      Lexer.new(sql_text).tokenize
    end
  end
end

