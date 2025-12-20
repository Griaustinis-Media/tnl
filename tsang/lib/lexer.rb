module Tsang
  class Token
    attr_reader :type, :value, :position

    def initialize(type, value, position = 0)
      @type = type
      @value = value
      @position = position
    end

    def to_s
      "Token(#{type}: #{value.inspect})"
    end
  end

  class Lexer
    KEYWORDS = %w[
      SELECT FROM WHERE AND OR NOT IN BETWEEN LIKE IS NULL
      INSERT INTO VALUES UPDATE SET DELETE DROP CREATE TABLE
      ALTER ADD COLUMN PRIMARY KEY FOREIGN REFERENCES
      JOIN LEFT RIGHT INNER OUTER ON AS ORDER BY GROUP HAVING
      LIMIT OFFSET DISTINCT ALL ASC DESC COUNT SUM AVG MAX MIN
    ].freeze

    OPERATORS = {
      '=' => :EQUALS,
      '!=' => :NOT_EQUALS,
      '<>' => :NOT_EQUALS,
      '<' => :LESS_THAN,
      '<=' => :LESS_THAN_OR_EQUAL,
      '>' => :GREATER_THAN,
      '>=' => :GREATER_THAN_OR_EQUAL,
      '+' => :PLUS,
      '-' => :MINUS,
      '*' => :ASTERISK,
      '/' => :DIVIDE,
      '%' => :MODULO
    }.freeze

    def initialize(input)
      @input = input
      @position = 0
      @tokens = []
    end

    def tokenize
      while @position < @input.length
        char = current_char

        case char
        when /\s/
          skip_whitespace
        when /[a-zA-Z_]/
          @tokens << identifier_or_keyword
        when /[0-9]/
          @tokens << number
        when "'"
          @tokens << string_literal
        when '"'
          @tokens << quoted_identifier
        when '('
          @tokens << Token.new(:LPAREN, '(', @position)
          advance
        when ')'
          @tokens << Token.new(:RPAREN, ')', @position)
          advance
        when ','
          @tokens << Token.new(:COMMA, ',', @position)
          advance
        when ';'
          @tokens << Token.new(:SEMICOLON, ';', @position)
          advance
        when '.'
          @tokens << Token.new(:DOT, '.', @position)
          advance
        when '=', '!', '<', '>', '+', '-', '*', '/', '%'
          @tokens << operator
        else
          raise "Unexpected character: #{char} at position #{@position}"
        end
      end

      @tokens << Token.new(:EOF, nil, @position)
      @tokens
    end

    private

    def current_char
      @input[@position]
    end

    def peek_char(offset = 1)
      pos = @position + offset
      pos < @input.length ? @input[pos] : nil
    end

    def advance
      @position += 1
    end

    def skip_whitespace
      advance while current_char&.match?(/\s/)
    end

    def identifier_or_keyword
      start_pos = @position
      value = ''

      while current_char&.match?(/[a-zA-Z0-9_]/)
        value << current_char
        advance
      end

      type = KEYWORDS.include?(value.upcase) ? value.upcase.to_sym : :IDENTIFIER
      Token.new(type, value, start_pos)
    end

    def number
      start_pos = @position
      value = ''
      has_decimal = false

      while current_char&.match?(/[0-9.]/)
        if current_char == '.'
          break if has_decimal

          has_decimal = true
        end
        value << current_char
        advance
      end

      Token.new(:NUMBER, value, start_pos)
    end

    def string_literal
      start_pos = @position
      advance # skip opening quote
      value = ''

      while current_char && current_char != "'"
        if current_char == '\\'
          advance
          value << current_char if current_char
        else
          value << current_char
        end
        advance
      end

      raise "Unterminated string at position #{start_pos}" unless current_char == "'"

      advance # skip closing quote

      Token.new(:STRING, value, start_pos)
    end

    def quoted_identifier
      start_pos = @position
      advance # skip opening quote
      value = ''

      while current_char && current_char != '"'
        value << current_char
        advance
      end

      raise "Unterminated quoted identifier at position #{start_pos}" unless current_char == '"'

      advance # skip closing quote

      Token.new(:IDENTIFIER, value, start_pos)
    end

    def operator
      start_pos = @position
      op = current_char.to_s
      advance

      # Check for two-character operators
      if current_char && OPERATORS.key?(op + current_char)
        op += current_char
        advance
      end

      type = OPERATORS[op]
      raise "Unknown operator: #{op}" unless type

      Token.new(type, op, start_pos)
    end
  end
end
