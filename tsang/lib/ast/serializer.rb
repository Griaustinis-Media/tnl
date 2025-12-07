module Tsang
  module AST
    module Serializer
      def self.to_hash(node)
        return nil if node.nil?
        return node if node.is_a?(Hash)
        
        case node
        when Tsang::AST::SelectStatement
          {
            type: :select,
            columns: node.columns.map { |c| to_hash(c) },
            from: to_hash(node.from),
            where: to_hash(node.where),
            joins: node.joins&.map { |j| to_hash(j) } || [],
            group_by: to_hash(node.group_by),
            having: to_hash(node.having),
            order_by: node.order_by&.map { |o| to_hash(o) } || [],
            limit: node.limit,
            offset: node.offset
          }.compact
        when Tsang::AST::InsertStatement
          { type: :insert, table: to_hash(node.table), columns: node.columns, values: node.values }
        when Tsang::AST::UpdateStatement
          { type: :update, table: to_hash(node.table), assignments: node.assignments.map { |a| to_hash(a) }, where: to_hash(node.where) }
        when Tsang::AST::DeleteStatement
          { type: :delete, table: to_hash(node.table), where: to_hash(node.where) }
        when Tsang::AST::InExpression
          {
            type: :in_expression,
            expression: to_hash(node.expression),
            values: node.values.map { |v| to_hash(v) },
            negated: node.negated
          }
        when Tsang::AST::TableReference
          node.schema ? { schema: node.schema, table: node.name } : { table: node.name }
        when Tsang::AST::ColumnReference
          { type: :column, name: node.name }
        when Tsang::AST::BinaryOp
          { type: :binary_op, operator: node.operator, left: to_hash(node.left), right: to_hash(node.right) }
        when Tsang::AST::UnaryOp
          { type: :unary_op, operator: node.operator, operand: to_hash(node.operand) }
        when Tsang::AST::FunctionCall
          { type: :function, name: node.name, arguments: node.arguments || [] }
        when Tsang::AST::Literal
          case node.data_type
          when :NUMBER
            { type: :number, value: node.value.to_i }
          when :STRING
            { type: :string, value: node.value }
          when :NULL
            { type: :null, value: nil }
          else
            { type: :literal, value: node.value }
          end
        when Tsang::AST::Join
          { type: node.type || :inner, table: to_hash(node.table), condition: to_hash(node.condition) }
        when Tsang::AST::OrderBy
          { expression: to_hash(node.expression), direction: node.direction || :asc }
        when Tsang::AST::GroupBy
          { expressions: node.expressions.map { |e| to_hash(e) } }
        when Tsang::AST::Assignment
          { column: node.column, value: to_hash(node.value) }
        when Symbol
          node == :* ? { type: :all } : node
        when String
          node == '*' ? { type: :all } : node
        when Integer, Float
          node
        else
          { type: :unknown, class: node.class.name, value: node.to_s }
        end
      end
    end
  end
end
