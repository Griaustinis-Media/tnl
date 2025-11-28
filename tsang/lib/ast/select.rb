require_relative 'node'

module Tsang
  module AST
    class SelectStatement < Node
      attr_reader :columns, :from, :where, :joins, :group_by, :having, :order_by, :limit, :offset
      
      def initialize(columns:, from: nil, where: nil, joins: [], group_by: nil, having: nil, order_by: nil, limit: nil, offset: nil)
        @columns = columns
        @from = from
        @where = where
        @joins = joins
        @group_by = group_by
        @having = having
        @order_by = order_by
        @limit = limit
        @offset = offset
      end
    end
  end
end
