require_relative 'lexer'
require_relative 'ast/ast'

module Tsang
  class Parser
    def initialize(tokens)
      @tokens = tokens
      @position = 0
    end

    def parse
      statements = []

      until current_token.type == :EOF
        statements << parse_statement
        consume(:SEMICOLON) if current_token.type == :SEMICOLON
      end

      statements.length == 1 ? statements.first : statements
    end

    private

    def match?(*types)
      types.include?(current_token.type)
    end

    def current_token
      @tokens[@position]
    end

    def peek_token(offset = 1)
      pos = @position + offset
      pos < @tokens.length ? @tokens[pos] : @tokens.last
    end

    def advance
      token = current_token
      @position += 1 unless current_token.type == :EOF
      token
    end

    def consume(expected_type)
      token = current_token
      unless token.type == expected_type
        raise "Expected #{expected_type} but got #{token.type} at position #{token.position}"
      end

      advance
      token
    end

    def parse_statement
      case current_token.type
      when :SELECT
        parse_select
      when :INSERT
        parse_insert
      when :UPDATE
        parse_update
      when :DELETE
        parse_delete
      else
        raise "Unexpected statement starting with #{current_token.type}"
      end
    end

    def parse_select
      consume(:SELECT)

      # Handle DISTINCT
      advance if current_token.type == :DISTINCT

      # Parse columns
      columns = parse_select_list

      # Parse FROM clause
      from = nil
      if current_token.type == :FROM
        advance
        from = parse_table_reference
      end

      # Parse JOINs
      joins = []
      joins << parse_join while %i[JOIN LEFT RIGHT INNER OUTER].include?(current_token.type)

      # Parse WHERE clause
      where = nil
      if current_token.type == :WHERE
        advance
        where = parse_expression
      end

      # Parse GROUP BY
      group_by = nil
      if current_token.type == :GROUP
        advance
        consume(:BY)
        group_by = AST::GroupBy.new(expressions: parse_expression_list)
      end

      # Parse HAVING
      having = nil
      if current_token.type == :HAVING
        advance
        having = parse_expression
      end

      # Parse ORDER BY
      order_by = nil
      if current_token.type == :ORDER
        advance
        consume(:BY)
        order_by = parse_order_by_list
      end

      # Parse LIMIT
      limit = nil
      if current_token.type == :LIMIT
        advance
        limit = consume(:NUMBER).value.to_i
      end

      # Parse OFFSET
      offset = nil
      if current_token.type == :OFFSET
        advance
        offset = consume(:NUMBER).value.to_i
      end

      AST::SelectStatement.new(
        columns: columns,
        from: from,
        where: where,
        joins: joins,
        group_by: group_by,
        having: having,
        order_by: order_by,
        limit: limit,
        offset: offset
      )
    end

    def parse_insert
      consume(:INSERT)
      consume(:INTO)

      table = parse_table_reference

      # Parse column list (optional)
      columns = []
      if current_token.type == :LPAREN
        advance
        columns = parse_identifier_list
        consume(:RPAREN)
      end

      consume(:VALUES)

      # Parse value lists
      values = []
      loop do
        consume(:LPAREN)
        values << parse_expression_list
        consume(:RPAREN)

        break unless current_token.type == :COMMA

        advance
      end

      AST::InsertStatement.new(
        table: table,
        columns: columns,
        values: values
      )
    end

    def parse_update
      consume(:UPDATE)

      table = parse_table_reference

      consume(:SET)

      assignments = []
      loop do
        column = consume(:IDENTIFIER).value
        consume(:EQUALS)
        value = parse_expression

        assignments << AST::Assignment.new(
          column: AST::ColumnReference.new(name: column),
          value: value
        )

        break unless current_token.type == :COMMA

        advance
      end

      where = nil
      if current_token.type == :WHERE
        advance
        where = parse_expression
      end

      AST::UpdateStatement.new(
        table: table,
        assignments: assignments,
        where: where
      )
    end

    def parse_delete
      consume(:DELETE)
      consume(:FROM)

      table = parse_table_reference

      where = nil
      if current_token.type == :WHERE
        advance
        where = parse_expression
      end

      AST::DeleteStatement.new(
        table: table,
        where: where
      )
    end

    def parse_select_list
      items = []

      loop do
        if current_token.type == :ASTERISK
          advance
          items << AST::ColumnReference.new(name: '*')
        else
          expr = parse_expression

          # Check for alias
          alias_name = nil
          if current_token.type == :AS
            advance
            alias_name = consume(:IDENTIFIER).value
          elsif current_token.type == :IDENTIFIER
            # Check that the next token is either COMMA, FROM, WHERE, or end of statement
            # This helps us distinguish aliases from keywords
            next_type = peek_token.type
            unless %i[EOF COMMA FROM WHERE GROUP HAVING ORDER LIMIT SEMICOLON].include?(next_type)
              alias_name = consume(:IDENTIFIER).value
            end
          end

          # Set alias if present
          if expr.is_a?(AST::ColumnReference) && alias_name
            expr = AST::ColumnReference.new(
              name: expr.name,
              table: expr.table,
              alias_name: alias_name
            )
          end

          items << expr
        end

        break unless current_token.type == :COMMA

        advance
      end

      items
    end

    def parse_table_reference
      schema = nil
      name_token = current_token

      # Must be an identifier to start
      raise "Expected identifier but got #{name_token.type}" unless name_token.type == :IDENTIFIER

      name = advance.value

      # Check for schema.table notation
      if current_token.type == :DOT
        advance
        schema = name
        name = consume(:IDENTIFIER).value
      end

      # Check for alias
      alias_name = nil
      if current_token.type == :AS
        advance
        alias_name = consume(:IDENTIFIER).value
      elsif current_token.type == :IDENTIFIER
        alias_name = consume(:IDENTIFIER).value
      end

      AST::TableReference.new(
        name: name,
        schema: schema,
        alias_name: alias_name
      )
    end

    def parse_join
      # Parse join type
      type = :INNER
      if current_token.type == :LEFT
        type = :LEFT
        advance
      elsif current_token.type == :RIGHT
        type = :RIGHT
        advance
      elsif current_token.type == :INNER
        advance
      elsif current_token.type == :OUTER
        type = :OUTER
        advance
      end

      consume(:JOIN)
      table = parse_table_reference

      # Parse ON condition
      condition = nil
      if current_token.type == :ON
        advance
        condition = parse_expression
      end

      AST::Join.new(
        type: type,
        table: table,
        condition: condition
      )
    end

    def parse_order_by_list
      items = []

      loop do
        expr = parse_expression

        direction = :ASC
        if current_token.type == :ASC
          advance
        elsif current_token.type == :DESC
          direction = :DESC
          advance
        end

        items << AST::OrderBy.new(
          expression: expr,
          direction: direction
        )

        break unless current_token.type == :COMMA

        advance
      end

      items
    end

    def parse_expression
      parse_or_expression
    end

    def parse_or_expression
      left = parse_and_expression

      while current_token.type == :OR
        op_token = advance
        op = map_operator(op_token.type)
        right = parse_and_expression
        left = AST::BinaryOp.new(left: left, operator: op, right: right)
      end

      left
    end

    def parse_and_expression
      left = parse_comparison_expression

      while current_token.type == :AND
        op_token = advance
        op = map_operator(op_token.type)
        right = parse_comparison_expression
        left = AST::BinaryOp.new(left: left, operator: op, right: right)
      end

      left
    end

    def parse_comparison_expression
      left = parse_additive_expression

      # Check for NOT IN first
      return parse_not_in_expression(left) if current_token.type == :NOT && peek_token.type == :IN

      # Check for IN
      return parse_in_expression(left) if current_token.type == :IN

      # Regular comparison operators
      while %i[EQUALS NOT_EQUALS LESS_THAN LESS_THAN_OR_EQUAL
               GREATER_THAN GREATER_THAN_OR_EQUAL LIKE].include?(current_token.type)
        op_token = advance
        op = map_operator(op_token.type)
        right = parse_additive_expression
        left = AST::BinaryOp.new(left: left, operator: op, right: right)
      end

      left
    end

    def parse_additive_expression
      left = parse_multiplicative_expression

      while %i[PLUS MINUS].include?(current_token.type)
        op_token = advance
        op = map_operator(op_token.type)
        right = parse_multiplicative_expression
        left = AST::BinaryOp.new(left: left, operator: op, right: right)
      end

      left
    end

    def parse_multiplicative_expression
      left = parse_unary_expression

      while %i[ASTERISK DIVIDE MODULO].include?(current_token.type)
        op_token = advance
        op = map_operator(op_token.type)
        right = parse_unary_expression
        left = AST::BinaryOp.new(left: left, operator: op, right: right)
      end

      left
    end

    def parse_unary_expression
      if current_token.type == :NOT
        op_token = advance
        op = map_operator(op_token.type)
        operand = parse_unary_expression
        return AST::UnaryOp.new(operator: op, operand: operand)
      end

      if current_token.type == :MINUS
        op_token = advance
        op = map_operator(op_token.type)
        operand = parse_unary_expression
        return AST::UnaryOp.new(operator: op, operand: operand)
      end

      parse_primary_expression
    end

    def parse_primary_expression
      case current_token.type
      when :NUMBER
        token = advance
        AST::Literal.new(value: token.value, data_type: :NUMBER)
      when :STRING
        token = advance
        AST::Literal.new(value: token.value, data_type: :STRING)
      when :NULL
        advance
        AST::Literal.new(value: nil, data_type: :NULL)
      when :ASTERISK
        # Handle * (typically in COUNT(*) or SELECT *)
        advance
        AST::ColumnReference.new(name: '*')
      when :IDENTIFIER, :COUNT, :SUM, :AVG, :MAX, :MIN, :DISTINCT
        # Could be column reference or function call
        name = advance.value

        if current_token.type == :LPAREN
          # Function call
          advance

          distinct = false
          if current_token.type == :DISTINCT
            distinct = true
            advance
          end

          args = current_token.type == :RPAREN ? [] : parse_expression_list
          consume(:RPAREN)

          AST::FunctionCall.new(name: name, arguments: args, distinct: distinct)
        elsif current_token.type == :DOT
          # Table.column reference
          advance
          column_token = current_token

          # Column name could be an identifier or asterisk
          if column_token.type == :IDENTIFIER
            column_name = advance.value
          elsif column_token.type == :ASTERISK
            advance
            column_name = '*'
          else
            raise "Expected column name after DOT but got #{column_token.type}"
          end

          AST::ColumnReference.new(name: column_name, table: name)
        else
          # Simple column reference
          AST::ColumnReference.new(name: name)
        end
      when :LPAREN
        advance
        expr = parse_expression
        consume(:RPAREN)
        expr
      else
        raise "Unexpected token: #{current_token.type}"
      end
    end

    def parse_expression_list
      items = []

      loop do
        items << parse_expression
        break unless current_token.type == :COMMA

        advance
      end

      items
    end

    def parse_identifier_list
      items = []

      loop do
        items << consume(:IDENTIFIER).value
        break unless current_token.type == :COMMA

        advance
      end

      items
    end

    def parse_in_expression(left_expr)
      consume(:IN)
      consume(:LPAREN)

      values = []
      loop do
        values << parse_expression
        break unless match?(:COMMA)

        consume(:COMMA)
      end

      consume(:RPAREN)

      AST::InExpression.new(
        expression: left_expr,
        values: values,
        negated: false
      )
    end

    def parse_not_in_expression(left_expr)
      consume(:NOT)
      consume(:IN)
      consume(:LPAREN)

      values = []
      loop do
        values << parse_expression
        break unless match?(:COMMA)

        consume(:COMMA)
      end

      consume(:RPAREN)

      AST::InExpression.new(
        expression: left_expr,
        values: values,
        negated: true
      )
    end

    def map_operator(token_type)
      case token_type
      when :EQUALS then '='
      when :NOT_EQUALS then '!='
      when :LESS_THAN then '<'
      when :LESS_THAN_OR_EQUAL then '<='
      when :GREATER_THAN then '>'
      when :GREATER_THAN_OR_EQUAL then '>='
      when :LIKE then 'LIKE'
      when :IN then 'IN'
      when :PLUS then '+'
      when :MINUS then '-'
      when :ASTERISK then '*'
      when :DIVIDE then '/'
      when :MODULO then '%'
      when :AND then 'AND'
      when :OR then 'OR'
      when :NOT then 'NOT'
      else token_type.to_s
      end
    end
  end
end
