class Foo
  include DataMapper::Resource

  property :id, Serial
  property :bar, String
  property :time, String, :default => '0'

end
