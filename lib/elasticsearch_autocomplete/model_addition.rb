module ElasticsearchAutocomplete
  module ModelAddition
    def self.included(base)
      base.send :extend, SingletonMethods
    end

    module SingletonMethods
      def ac_setup_es
        include InstanceMethods
        include Elasticsearch::Model
        include Elasticsearch::Model::Adapter::ActiveRecord

        if ElasticsearchAutocomplete.defaults[:commit_callbacks]
          after_commit -> { ac_store_document(:index) }, on: :create
          after_commit -> { ac_store_document(:update) }, on: :update
          after_commit -> { ac_store_document(:delete) }, on: :destroy
        else
          after_create -> { ac_store_document(:index) }
          after_update -> { ac_store_document(:update) }
          after_destroy -> { ac_store_document(:delete) }
        end

        # no index_prefix anymore https://github.com/elasticsearch/elasticsearch-rails/issues/2
        if ElasticsearchAutocomplete.defaults[:index_prefix] && !index_name.start_with?(ElasticsearchAutocomplete.defaults[:index_prefix])
          index_name "#{ElasticsearchAutocomplete.defaults[:index_prefix]}_#{index_name}"
        end
      end

      def ac_field(*args)
        extend ClassMethods

        ac_setup_es

        class_attribute :ac_opts, :ac_attr, :ac_search_attrs, :ac_search_fields, :ac_mode_config, instance_writer: false
        options = args.extract_options!
        self.ac_opts = options.reverse_merge(ElasticsearchAutocomplete.defaults)
        self.ac_attr = args.first || ElasticsearchAutocomplete.defaults[:attr]

        self.ac_mode_config = ElasticsearchAutocomplete::MODES[ac_opts[:mode]]

        self.ac_search_attrs = ac_opts[:search_fields] ||
          if ac_opts[:localized]
            (I18n.try(:es_available_locales) || I18n.available_locales).map { |l| "#{ac_attr}_#{l}" }
          else
            [ac_attr]
          end
        self.ac_search_fields =
          ac_search_attrs.flat_map do |attr|
            ac_mode_config.values.map do |prefix|
              ["#{attr}.#{prefix}_#{attr}".to_sym, "#{prefix}_#{attr}".to_sym]
            end.flatten
          end
        self.ac_search_fields.push(*self.ac_search_attrs)

        define_ac_index(ac_opts[:mode]) unless options[:skip_settings]
      end
    end

    module ClassMethods
      def ac_search(query, options={})
        options.reverse_merge!({per_page: 50, search_fields: ac_search_fields})

        query =
          if query.size.zero?
            {match_all: {}}
          else
            {bool: {must: {multi_match: {query: query, fields: options[:search_fields]}}}}
          end

        sort = []
        if options[:geo_order] && options[:with]
          lat = options[:with].delete('lat').presence
          lon = options[:with].delete('lon').presence
          if lat && lon
            sort << {_geo_distance: {lat_lon: [lat, lon].join(','), order: 'asc', unit: 'km'}}
          end
        end
        sort << {options[:order] => options[:sort_mode] || 'asc'} if options[:order].present?

        filter = []

        options[:with].to_a.each do |k, v|
          k, v = [k.keys.first, k.values.first] if k.is_a?(Hash)
          filter << {terms: {k => ElasticsearchAutocomplete.val_to_terms(v, false, detect_field_type(k))}}
        end

        options[:without].to_a.each do |k, v|
          k, v = [k.first, k.last] if k.is_a?(Array)
          filter << {bool: {must_not: {terms: {k => ElasticsearchAutocomplete.val_to_terms(v, true, detect_field_type(k))}}}}
        end

        per_page = options[:per_page] || 50
        page = options[:page].presence || 1
        # from = per_page.to_i * (page.to_i - 1)

        if filter.present?
          query[:bool] ||= {}
          query[:bool][:filter] = filter
          query.delete(:match_all)
        end

        __elasticsearch__.search(query: query, sort: sort).paginate(page: page, size: per_page)
      end

      def detect_field_type(field)
        [field.to_sym, field.to_s].each do |formatted_field|
          r = field_mapping(formatted_field)
          type = r.try(:[], :type) || r.try(:type)
          return type if type
        end
        nil
      end

      def field_mapping(field)
        try(:mapping).try(:instance_variable_get, :@mapping).try(:[], field) ||
          try(:columns_hash).try(:[], field)
      end

      def define_ac_index(mode=:word)
        model = self
        model_ac_search_attrs = model.ac_search_attrs
        settings ElasticsearchAutocomplete::Analyzers::AC_BASE do
          mapping do
            model_ac_search_attrs.each do |attr|
              indexes attr, model.ac_index_config(attr, mode)
            end
          end
        end
      end

      def ac_index_config(attr, mode=:word)
        defaults = {type: :text, search_analyzer: :ac_search}
        fields = case mode
                   when :word
                     {
                         attr => {type: :text},
                         "#{ac_mode_config[:base]}_#{attr}" => defaults.merge(analyzer: :ac_edge_ngram),
                         "#{ac_mode_config[:word]}_#{attr}" => defaults.merge(analyzer: :ac_edge_ngram_word)
                     }
                   when :phrase
                     {
                         attr => {type: :text},
                         "#{ac_mode_config[:base]}_#{attr}" => defaults.merge(analyzer: :ac_edge_ngram)
                     }
                   when :full
                     {
                         attr => {type: :text},
                         "#{ac_mode_config[:base]}_#{attr}" => defaults.merge(analyzer: :ac_edge_ngram, boost: 3),
                         "#{ac_mode_config[:full]}_#{attr}" => defaults.merge(analyzer: :ac_edge_ngram_full)
                     }
                 end
        {fields: fields}
      end

    end

    module InstanceMethods
      def as_indexed_json(*)
        attrs = [:id, :created_at] + self.class.ac_search_attrs
        attrs.each_with_object({}) { |attr, json| json[attr] = send(attr) }
      end

      def ac_store_document(action)
        return true unless ElasticsearchAutocomplete.enable_indexing
        __elasticsearch__.send("#{action}_document")
      end
    end
  end
end
