require_relative 'tsang'
require_relative 'ast/ast'
require_relative 'ast/serializer'
require_relative 'codegen/source_config_factory'
require_relative 'codegen/pipeline_generator'
require_relative 'codegen/template_renderer'
require 'optparse'
require 'json'

module Tsang
  class CLI
    def self.run(args)
      if args.first && !args.first.start_with?('-')
        command = args.shift.to_sym
        options = parse_options(args)
        options[:command] = command
      else
        options = parse_options(args)
      end

      unless options[:command]
        show_help
        exit 1
      end

      case options[:command]
      when :parse then parse_command(options)
      when :generate then generate_command(options)
      when :batch_generate then batch_generate_command(options)
      else
        puts "Unknown command: #{options[:command]}"
        exit 1
      end
    end

    def self.parse_options(args)
      options = {}
      OptionParser.new do |opts|
        opts.banner = 'Usage: tsang [command] [options]'
        opts.on('-s', '--sql SQL', 'SQL query string') { |s| options[:sql] = s }
        opts.on('-f', '--file FILE', 'SQL file') { |f| options[:sql_file] = f }
        opts.on('-a', '--ast FILE', 'AST JSON file') { |a| options[:ast_file] = a }
        opts.on('-o', '--output DIR', 'Output directory') { |o| options[:output] = o }
        opts.on('-n', '--name NAME', 'Project name') { |n| options[:project_name] = n }
        opts.on('--config FILE', 'Config JSON file') { |cf| options[:config_file] = cf }
        opts.on('--batch FILE', 'Batch JSON file') { |bf| options[:batch_file] = bf }
        opts.on('--pretty', 'Pretty print JSON') { options[:pretty] = true }
        opts.on('-h', '--help', 'Show help') do
          show_help
          exit
        end
      end.parse!(args)
      options
    end

    def self.show_help
      puts <<~HELP
        Usage: tsang [command] [options]

        Commands:
          parse              Parse SQL to AST JSON
          generate           Generate pipeline from SQL or AST
          batch_generate     Generate multiple pipelines

        Options:
          -s, --sql SQL      SQL query string
          -f, --file FILE    SQL file
          -o, --output DIR   Output directory
          -n, --name NAME    Project name
          --pretty           Pretty print JSON
      HELP
    end

    def self.parse_command(options)
      sql = get_sql_input(options) or abort 'Error: Must provide SQL via --sql or --file'
      ast = Tsang.parse(sql)
      ast_hash = Tsang::AST::Serializer.to_hash(ast)
      puts options[:pretty] ? JSON.pretty_generate(ast_hash) : JSON.generate(ast_hash)
    rescue StandardError => e
      abort "Parse error: #{e.message}"
    end

    def self.generate_command(options)
      ast = if options[:ast_file]
              load_ast(options[:ast_file])
            else
              sql = get_sql_input(options) or abort 'Error: Must provide SQL or AST'
              Tsang::AST::Serializer.to_hash(Tsang.parse(sql))
            end

      config = load_config(options[:config_file])
      config[:project_name] = options[:project_name] || config[:project_name] || derive_project_name(ast)
      config[:original_sql] = sql || config[:original_sql] || '-- SQL not provided'

      generator = Tsang::Codegen::PipelineGenerator.new(ast, config)
      pipeline_data = generator.generate
      pipeline_data[:original_sql] = config[:original_sql]

      template_dir = File.expand_path('../templates', __dir__)
      output_dir = options[:output] || "./build/#{config[:project_name]}" # ← CHANGED

      renderer = Tsang::Codegen::TemplateRenderer.new(template_dir, output_dir)
      puts "\nGenerating pipeline: #{config[:project_name]}"
      renderer.render_project(pipeline_data)
      puts "\n✓ Pipeline generated in: #{output_dir}"
    rescue StandardError => e
      abort "Error: #{e.message}"
    end

    def self.batch_generate_command(options)
      batch_config = load_batch_config(options[:batch_file]) or abort 'Error: --batch required'
      base_output_dir = options[:output] || './build/pipelines' # ← CHANGED

      puts "\nGenerating #{batch_config[:pipelines].length} pipelines..."

      batch_config[:pipelines].each_with_index do |pipeline_config, index|
        puts "\n[#{index + 1}/#{batch_config[:pipelines].length}] #{pipeline_config[:name]}"

        ast = if pipeline_config[:ast_file]
                load_ast(pipeline_config[:ast_file])
              elsif pipeline_config[:sql]
                Tsang::AST::Serializer.to_hash(Tsang.parse(pipeline_config[:sql]))
              elsif pipeline_config[:sql_file]
                Tsang::AST::Serializer.to_hash(Tsang.parse(File.read(pipeline_config[:sql_file])))
              else
                puts '  ✗ Skipping: no SQL or AST provided'
                next
              end

        config = pipeline_config[:config] || {}
        config[:project_name] = pipeline_config[:name]
        config[:original_sql] = pipeline_config[:sql] || '-- SQL not provided'

        generator = Tsang::Codegen::PipelineGenerator.new(ast, config)
        renderer = Tsang::Codegen::TemplateRenderer.new(
          File.expand_path('../templates', __dir__),
          File.join(base_output_dir, config[:project_name])
        )
        renderer.render_project(generator.generate.merge(original_sql: config[:original_sql]))
      end

      puts "\n✓ All pipelines generated in: #{base_output_dir}"
    rescue StandardError => e
      abort "Error: #{e.message}"
    end

    def self.get_sql_input(options)
      options[:sql] || (options[:sql_file] && File.read(options[:sql_file]))
    end

    def self.load_ast(file)
      JSON.parse(File.read(file), symbolize_names: true)
    end

    def self.load_config(file)
      file ? JSON.parse(File.read(file), symbolize_names: true) : {}
    end

    def self.load_batch_config(file)
      file && JSON.parse(File.read(file), symbolize_names: true)
    end

    def self.derive_project_name(ast)
      from = ast[:from]
      table = from[:table].to_s.gsub('_', '-')
      from[:schema] ? "#{from[:schema].to_s.gsub('_', '-')}-#{table}-pipeline" : "#{table}-pipeline"
    end
  end
end
