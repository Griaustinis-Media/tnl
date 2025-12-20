require_relative 'node'

module Tsang
  module AST
    class UpdateStatement < Node
      attr_reader :table, :assignments, :where

      def initialize(table:, assignments:, where: nil)
        @table = table
        @assignments = assignments
        @where = where
      end
    end
  end
end
