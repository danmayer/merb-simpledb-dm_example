require 'rubygems'
require 'dm-core'
require 'aws_sdb'
require 'digest/sha1'
require 'dm-aggregates'
 
module DataMapper
  module Adapters
    class SimpleDBAdapter < AbstractAdapter

      def create(resources)
        created = 0
        resources.each do |resource|
          item_name = item_name_for_resource(resource)
          sdb_type = simpledb_type(resource.model)
          attributes = resource.attributes.merge(:simpledb_type => sdb_type)
          sdb.put_attributes(domain, item_name, attributes)
          created += 1
        end
        created
      end
      
      def delete(query)
        deleted = 0
        item_name = item_name_for_query(query)
        sdb.delete_attributes(domain, item_name)
        deleted += 1
        raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(query)
        deleted
      end
      
      def read_many(query)
        sdb_type = simpledb_type(query.model)
        
        conditions = ["['simpledb_type' = '#{sdb_type}']"]
        if query.conditions.size > 0
          conditions += query.conditions.map do |condition|
            operator = case condition[0]
              when :eql then '='
              when :not then '!='
              when :gt then '>'
              when :gte then '>='
              when :lt then '<'
              when :lte then '<='
              else raise "Invalid query operator: #{operator.inspect}"
            end
            "['#{condition[1].name.to_s}' #{operator} '#{condition[2].to_s}']"
          end
        end
        
        results = sdb.query(domain, conditions.compact.join(' intersection '))
        results = results[0].map {|d| sdb.get_attributes(domain, d) }
        
        Collection.new(query) do |collection|
          results.each do |result|
            data = query.fields.map do |property|
              value = result[property.field.to_s]
              if value != nil
                if value.size > 1
                  value.map {|v| property.typecast(v) }
                else
                  property.typecast(value[0])
                end
              else
                 property.typecast(nil)
              end
            end
            collection.load(data)
          end
        end
      end
      
      def read_one(query)
        sdb_type = simpledb_type(query.model)
        
        conditions = ["['simpledb_type' = '#{sdb_type}']"]
        if query.conditions.size > 0
          conditions += query.conditions.map do |condition|
            operator = case condition[0]
              when :eql then '='
              when :not then '!='
              when :gt then '>'
              when :gte then '>='
              when :lt then '<'
              when :lte then '<='
              else raise "Invalid query operator: #{operator.inspect}"
            end
            "['#{condition[1].name.to_s}' #{operator} '#{condition[2].to_s}']"
          end
        end
        
        results = sdb.query(domain, conditions.compact.join(' intersection '))
        results = results[0].map {|d| sdb.get_attributes(domain, d) }
        data = results[0]

        unless data==nil || data.empty?
          data = query.fields.map do |property|
            value = data[property.field.to_s]
            if value != nil
              if value.size > 1
                value.map {|v| property.typecast(v) }
              else
                  property.typecast(value[0])
              end
            else
              property.typecast(nil)
            end
          end
          
          query.model.load(data, query)
        end

      end
 
      def update(attributes, query)
        updated = 0
        item_name = item_name_for_query(query)
        attributes = attributes.to_a.map {|a| [a.first.name.to_s, a.last]}.to_hash
        sdb.put_attributes(domain, item_name, attributes, true)
        updated += 1
        raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(query)
        updated
      end
      
    private
      
      # Returns the domain for the model
      def domain
        @uri[:domain]
      end
      
      # Creates an item name for a query
      def item_name_for_query(query)
        sdb_type = simpledb_type(query.model)
        
        item_name = "#{sdb_type}+"
        keys = keys_for_model(query.model)
        conditions = query.conditions.sort {|a,b| a[1].name.to_s <=> b[1].name.to_s }
        item_name += conditions.map do |property|
          property[2].to_s
        end.join('-')
        Digest::SHA1.hexdigest(item_name)
      end
      
      # Creates an item name for a resource
      def item_name_for_resource(resource)
        sdb_type = simpledb_type(resource.model)
        
        item_name = "#{sdb_type}+"
        keys = keys_for_model(resource.model)
        item_name += keys.map do |property|
          resource.instance_variable_get(property.instance_variable_name)
        end.join('-')
        
        Digest::SHA1.hexdigest(item_name)
      end
      
      # Returns the keys for model sorted in alphabetical order
      def keys_for_model(model)
        model.key(self.name).sort {|a,b| a.name.to_s <=> b.name.to_s }
      end
      
      def not_eql_query?(query)
        # Curosity check to make sure we are only dealing with a delete
        conditions = query.conditions.map {|c| c[0] }.uniq
        selectors = [ :gt, :gte, :lt, :lte, :not, :like, :in ]
        return (selectors - conditions).size != selectors.size
      end
      
      # Returns an SimpleDB instance to work with
      def sdb
        @sdb ||= AwsSdb::Service.new(
                                     :access_key_id => @uri[:access_key_id],
                                     :secret_access_key => @uri[:secret_access_key]
                                     )
        @sdb
      end
      
      # Returns a string so we know what type of
      def simpledb_type(model)
        model.storage_name(model.repository.name)
      end

      #borrowed and edited from http://github.com/edward/dm-simpledb/tree/master
      module Migration
        # Returns whether the storage_name exists.
        # @param storage_name<String> a String defining the name of a domain
        # @return <Boolean> true if the storage exists
        def storage_exists?(storage_name)
          sdb.domains.detect {|d| d.name == storage_name }
        end
        
        def create_model_storage(repository, model)
          sdb.create_domain(@uri[:domain])
        end
        
        #On SimpleDB you probably don't want to destroy the whole domain
        #if you are just adding fields it is automatically supported
        #default to non destructive migrate, to destroy run
        #rake db:automigrate destroy=true
        def destroy_model_storage(repository, model)
          if ENV['destroy']!=nil && ENV['destroy']=='true'
            sdb.delete_domain(@uri[:domain])
          end
        end
        
        #TODO look at github panda simpleDB for serials support?
        module SQL
          def supports_serial?
            false
          end
        end
        
        include SQL
        
      end # module Migration
      
      include Migration
      
    end # class SimpleDBAdapter
    
    # Required naming scheme.
    SimpledbAdapter = SimpleDBAdapter
    
  end # module Adapters
end # module DataMapper
