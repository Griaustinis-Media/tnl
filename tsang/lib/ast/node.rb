module Tsang
  module AST
    class Node
      def accept(visitor)
        visitor.visit(self)
      end

      def to_h
        hash = { type: self.class.name.split('::').last }
        instance_variables.each do |var|
          key = var.to_s.delete('@').to_sym
          value = instance_variable_get(var)
          hash[key] = serialize_value(value)
        end
        hash
      end

      private

      def serialize_value(value)
        case value
        when Node
          value.to_h
        when Array
          value.map { |v| serialize_value(v) }
        else
          value
        end
      end
    end
  end
end
