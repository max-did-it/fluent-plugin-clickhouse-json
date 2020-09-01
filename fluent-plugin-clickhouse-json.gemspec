Gem::Specification.new do |s|
  s.name        = 'Fluent Plugin Clickhouse-json'
  s.version     = '0.1'
  s.authors     = ['Max Bezlegkiy']
  s.email       = ['max-bez-l@mail.ru']
  s.homepage    = 'https://github.com/max-did-it'
  s.summary     = 'Plugin for fluent-d for output to clickhouse'
  s.files       = ['out_clickhouse_json.rb']
  s.add_dependency 'fluent'
end
