require "fx/function"

module Fx
  module Adapters
    class Postgres
      # Fetches defined functions from the postgres connection.
      # @api private
      class Functions
        # The SQL query used by F(x) to retrieve the functions considered
        # dumpable into `db/schema.rb`.
        FUNCTIONS_WITH_DEFINITIONS_QUERY = <<-EOS.freeze
          SELECT
            pp.proname AS name,
            
            case when pa.aggfnoid is not null
              then
              format('CREATE AGGREGATE %s (SFUNC = %s, STYPE = %s%s%s%s%s)'
                  , aggfnoid::regprocedure
                  , aggtransfn
                  , aggtranstype::regtype
                  , ', SORTOP = '    || NULLIF(aggsortop, 0)::regoper
                  , ', INITCOND = '  || agginitval
                  , ', FINALFUNC = ' || NULLIF(aggfinalfn, 0)
                  , CASE WHEN aggfinalextra THEN ', FINALFUNC_EXTRA' END
                    ) 
              else
              pg_get_functiondef(pp.oid)
            end
          
                AS definition
              
          FROM pg_proc pp
          JOIN pg_namespace pn
              ON pn.oid = pp.pronamespace
          LEFT JOIN pg_depend pd
              ON pd.objid = pp.oid AND pd.deptype = 'e'
          LEFT JOIN pg_aggregate pa
              ON pa.aggfnoid = pp.oid
              
          WHERE pn.nspname = 'public' AND pd.objid IS null
          ORDER BY pp.oid;
        EOS

        # Wraps #all as a static facade.
        #
        # @return [Array<Fx::Function>]
        def self.all(*args)
          new(*args).all
        end

        def initialize(connection)
          @connection = connection
        end

        # All of the functions that this connection has defined.
        #
        # @return [Array<Fx::Function>]
        def all
          functions_from_postgres.map { |function| to_fx_function(function) }
        end

        private

        attr_reader :connection

        def functions_from_postgres
          connection.execute(FUNCTIONS_WITH_DEFINITIONS_QUERY)
        end

        def to_fx_function(result)
          Fx::Function.new(result)
        end
      end
    end
  end
end
