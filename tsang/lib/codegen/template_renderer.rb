require 'liquid'
require 'fileutils'

module Tsang
  module Codegen
    class TemplateRenderer
      attr_reader :template_dir, :output_dir

      def initialize(template_dir, output_dir)
        @template_dir = template_dir
        @output_dir = output_dir
      end

      def render(template_name, context)
        template_path = File.join(template_dir, "#{template_name}.liquid")
        template_content = File.read(template_path)
        template = Liquid::Template.parse(template_content)
        template.render(context)
      end

      def render_project(pipeline_data)
        FileUtils.mkdir_p(output_dir)
        
        context = build_context(pipeline_data)
        
        files = {
          'deps.edn' => 'pipeline/deps.edn',
          'src/pipeline.clj' => 'pipeline/pipeline.clj',
          'README.md' => 'pipeline/README.md',
          'resources/logback.xml' => 'pipeline/logback.xml'
        }
        
        files.each do |output_path, template_name|
          full_output_path = File.join(output_dir, output_path)
          FileUtils.mkdir_p(File.dirname(full_output_path))
          
          content = render(template_name, context)
          File.write(full_output_path, content)
          
          puts "  ✓ Generated: #{output_path}"
        end
      end

      private

      def build_context(pipeline_data)
        {
          'project_name' => pipeline_data[:config][:project_name],
          'namespace' => pipeline_data[:config][:project_name].gsub('-', '_'),
          'source' => stringify_keys(pipeline_data[:source]),
          'sink' => stringify_keys(pipeline_data[:sink]),
          'columns' => pipeline_data[:columns],
          'conditions' => pipeline_data[:conditions].map { |c| stringify_keys(c) },
          'timestamp_column' => pipeline_data[:timestamp_column],
          'batch_size' => pipeline_data[:config][:batch_size],
          'watermark_enabled' => pipeline_data[:config][:watermark_enabled],
          'incremental' => pipeline_data[:config][:incremental],
          'leng_path' => pipeline_data[:config][:leng_path] || '../../leng',
          'original_sql' => pipeline_data[:original_sql]
        }
      end

      def stringify_keys(hash)
        return hash unless hash.is_a?(Hash)
        hash.transform_keys(&:to_s).transform_values do |v|
          v.is_a?(Hash) ? stringify_keys(v) : v
        end
      end
    end
  end
end
