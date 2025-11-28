require_relative 'node'

module Tsang
  module AST
    class Literal < Node
      attr_reader :value, :data_type
      
      def initialize(value:, data_type:)
        @value = value
        @data_type = data_type
      end
    end
  end
end
