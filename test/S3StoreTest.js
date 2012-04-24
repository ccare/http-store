var sv = require('../lib/SpliceValidator')
var assert = require('assert')

var SpliceValidator = sv.SpliceValidator
var ValidationError = sv.ValidationError

suite('S3StoreTest', function() {
    
    setup(function() {
        
    });

    suite('#validate()', function() {
        test('should throw an error when config is null', function() {
            assertValidationErrorThrown(null)
        });
        test('should throw an error when config is undefined', function() {
            assertValidationErrorThrown(undefined)
        });
        test('should throw an error when config is empty', function() {
            assertValidationErrorThrown({})
        });
        
        // target_bucket
        test('should throw an error when target_bucket is null', function() {
            config.target_bucket = null
            assertValidationErrorThrown(config)
        });
        test('should throw an error when target_bucket is undefined', function() {
            config.target_bucket = undefined
            assertValidationErrorThrown(config)
        });
        test('should throw an error when target_bucket is numeric', function() {
            config.target_bucket = 1
            assertValidationErrorThrown(config)
        });
        test('should throw an error when target_bucket doesnt match pattern', function() {
            config.target_bucket = "b"
            assertValidationErrorThrown(config)
        });
        test('target_bucket can contain characters and hyphens', function() {
            config.target_bucket = "b-slug-value"
            validator.validate(config)
            config.target_bucket = "b-master"
            validator.validate(config)
            config.target_bucket = "b-slug-001023"
            validator.validate(config)
            config.target_bucket = "b-slug2-abcd123"
            validator.validate(config)
        });
        
        // Sources json tests
        test('should throw an error when sources is null', function() {
            config.sources = null
            assertValidationErrorThrown(config)
        });
        test('should throw an error when sources is undefined', function() {
            config.sources = undefined
            assertValidationErrorThrown(config)
        });
        test('should throw an error when sources is not an array', function() {
            config.sources = "not an array"
            assertValidationErrorThrown(config)
            config.sources = {}
            assertValidationErrorThrown(config)
        });
        test('should throw an error when sources is empty', function() {
            config.sources = []
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with null origin_bucket', function() {
            config.sources[0].origin_bucket = null
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with undefined origin_bucket', function() {
            config.sources[0].origin_bucket = undefined
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with malformed origin_bucket', function() {
            config.sources[0].origin_bucket = "b-bucket with spaces"
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with null target_graph', function() {
            config.sources[0].target_graph = null
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with undefined target_graph', function() {
            config.sources[0].target_graph = undefined
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with malformed target_graph', function() {
            config.sources[0].target_graph = "   http:// not A graph!"
            assertValidationErrorThrown(config)
        });
        test('valid graphs should work', function() {
            config.sources[0].target_graph = "http://data.kasabi.com/foo/bar"
            validator.validate(config)
        });
        
        // Sources query field
        test('should error when sources contains entry with null query', function() {
            config.sources[0].query = null
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with undefined query', function() {
            config.sources[0].query = undefined
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with empty query', function() {
            config.sources[0].query = "           "
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with malformed query', function() {
            config.sources[0].query = "djfilhasfkjdashfkldhas"
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with non-describe-or-construct query', function() {
            config.sources[0].query = "select ?s where {?s ?p ?o} "
            assertValidationErrorThrown(config)
        });
        test('should error when sources contains entry with non-describe-or-construct query', function() {
            config.sources[0].query = "ask where {?s ?p ?o} "
            assertValidationErrorThrown(config)
        });
        test('describe queries should be valid', function() {
            config.sources[0].query = "describe ?s where {?s ?p ?o} "
            validator.validate(config)
        });
        test('construct queries should be valid', function() {
            config.sources[0].query = "construct {?s ?p ?o} where {?s ?p ?o} "
            validator.validate(config)
        });
        
        // Complex unhappy paths        
        test('should error when one source is good but other is bad', function() {
            assertValidationErrorThrown({
                target_bucket: "b-slug-target",
                sources: [
                    {
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    },
                    {
                        origin_bucket: "b-slug-origin2",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    }
                ]
            })
        });    
        test('should error when one source is good but other is bad (2)', function() {
            assertValidationErrorThrown({
                target_bucket: "b-slug-target",
                sources: [
                    {
                        origin_bucket: "b-slug-origin1",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    },
                    {
                        origin_bucket: "b-slug-origin2",
                        query: "",
                        target_graph: "http://data.kasabi.com/mygraph"
                    }
                ]
            })
        }); 
        test('target and source buckets cannot match', function() {
            assertValidationErrorThrown({
                target_bucket: "b-slug-target",
                sources: [
                    {
                        origin_bucket: "b-slug-target",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    }
                ]
            })
        });
        test('target and source buckets cannot match (2)', function() {
            assertValidationErrorThrown({
                target_bucket: "b-slug-target",
                sources: [
                    {
                        origin_bucket: "b-slug-origin1",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    },
                    {
                        origin_bucket: "b-slug-target",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    }
                ]
            })
        });
            
      
        // Happy paths
        test('should not throw when config ok', function() {
            validator.validate(config)
        });
        test('Multiple sources work', function() {
            validator.validate({
                target_bucket: "b-slug-target",
                sources: [
                    {
                        origin_bucket: "b-slug-origin1",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    },
                    {
                        origin_bucket: "b-slug-origin2",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph2"
                    }
                ]
            })
        });
        test('Multiple sources work', function() {
            validator.validate({
                target_bucket: "b-slug-target",
                sources: [
                    {
                        origin_bucket: "b-slug-origin1",
                        query: "describe ?s where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph"
                    },
                    {
                        origin_bucket: "b-slug-origin2",
                        query: "construct {?s ?p ?o} where {?s ?p ?o}",
                        target_graph: "http://data.kasabi.com/mygraph2"
                    }
                ]
            })
        });
        
        function assertValidationErrorThrown(configValue) {    
            assert.throws( function() {
                validator.validate( configValue )
            }, ValidationError)
        }
    });
});

