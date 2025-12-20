require_relative 'node'

module Tsang
  module AST
    class DeleteStatement < Node
      attr_reader :table, :where

      def initialize(table:, where: nil)
        @table = table
        @where = where
      end
    end
  end
end
