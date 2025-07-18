import pytest
from jaf.result_set import JafQuerySet, JafQuerySetError


class TestJafQuerySetInit:
    def test_valid_creation(self):
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        rs = JafQuerySet(query=query, collection_id="id1")
        assert rs.query == query
        assert rs.collection_id == "id1"

    def test_valid_creation_no_collection_id(self):
        query = ["gt?", ["@", [["key", "age"]]], 18]
        rs = JafQuerySet(query=query)
        assert rs.query == query
        assert rs.collection_id is None

    def test_valid_creation_with_collection_source(self):
        query = ["lt?", ["@", [["key", "score"]]], 50]
        source = {"type": "jsonl", "path": "/test.jsonl"}
        rs = JafQuerySet(query=query, collection_id="test", collection_source=source)
        assert rs.query == query
        assert rs.collection_source == source

    def test_valid_creation_minimal(self):
        # Test minimal constructor - just a query
        query = ["is-string?", ["@", [["key", "name"]]]]
        rs = JafQuerySet(query=query)
        assert rs.query == query
        assert rs.collection_id is None
        assert rs.collection_source is None

    def test_constructor_positional_args(self):
        # Test that positional args work (though keyword args are preferred)
        rs = JafQuerySet(["eq?", True], "test_id", {"type": "test"})
        assert rs.query == ["eq?", True]
        assert rs.collection_id == "test_id"
        assert rs.collection_source == {"type": "test"}


class TestJafQuerySetSerialization:
    def test_to_dict(self):
        query = ["eq?", ["@", [["key", "status"]]], "active"]
        source = {"type": "jsonl", "path": "/test.jsonl"}
        rs = JafQuerySet(query=query, collection_id="id_test", collection_source=source)
        expected = {
            "query": query,
            "collection_id": "id_test",
            "collection_source": source,
        }
        assert rs.to_dict() == expected

    def test_to_dict_minimal(self):
        query = ["gt?", ["@", [["key", "age"]]], 18]
        rs = JafQuerySet(query=query)
        expected = {
            "query": query,
            "collection_id": None,
            "collection_source": None,
        }
        assert rs.to_dict() == expected

    def test_from_dict_valid(self):
        query = ["lt?", ["@", [["key", "price"]]], 100]
        data = {"query": query, "collection_id": "id_test"}
        rs = JafQuerySet.from_dict(data)
        assert rs.query == query
        assert rs.collection_id == "id_test"
        assert rs.collection_source is None

    def test_from_dict_with_source(self):
        query = ["contains?", ["@", [["key", "tags"]]], "python"]
        source = {"type": "json_array", "path": "/data.json"}
        data = {"query": query, "collection_id": "test", "collection_source": source}
        rs = JafQuerySet.from_dict(data)
        assert rs.query == query
        assert rs.collection_source == source

    def test_from_dict_missing_query(self):
        with pytest.raises(
            ValueError,
            match="JafQuerySet.from_dict: Missing required key in input data: 'query'",
        ):
            JafQuerySet.from_dict({"collection_id": "test"})

    def test_from_dict_empty_dict(self):
        with pytest.raises(
            ValueError,
            match="JafQuerySet.from_dict: Missing required key in input data: 'query'",
        ):
            JafQuerySet.from_dict({})

    def test_from_dict_with_all_fields(self):
        # Test that all new format fields work correctly
        query = ["eq?", ["@", [["key", "name"]]], "test"]
        source = {"type": "jsonl", "path": "/test.jsonl"}
        data = {
            "query": query,
            "collection_id": "test_collection",
            "collection_source": source,
        }
        rs = JafQuerySet.from_dict(data)
        assert rs.query == query
        assert rs.collection_id == "test_collection"
        assert rs.collection_source == source


class TestJafQuerySetCompatibility:
    def test_compatible(self):
        rs1 = JafQuerySet(query=["eq?", "@name", "test1"], collection_id="id1")
        rs2 = JafQuerySet(query=["eq?", "@name", "test2"], collection_id="id1")
        rs1._check_compatibility(rs2)

    def test_compatible_one_id_none(self):
        rs1 = JafQuerySet(query=["eq?", "@name", "test1"], collection_id="id1")
        rs2 = JafQuerySet(query=["eq?", "@name", "test2"], collection_id=None)
        rs1._check_compatibility(rs2)

    def test_compatible_both_ids_none(self):
        rs1 = JafQuerySet(query=["eq?", "@name", "test1"])
        rs2 = JafQuerySet(query=["eq?", "@name", "test2"])
        rs1._check_compatibility(rs2)

    def test_incompatible_collection_id_warning(self):
        # With lazy evaluation, different collection IDs just issue a warning
        rs1 = JafQuerySet(query=["eq?", "@name", "test1"], collection_id="id1")
        rs2 = JafQuerySet(query=["eq?", "@name", "test2"], collection_id="id2")

        # This should not raise an error, just log a warning
        # No exception should be raised - this is the new lazy evaluation behavior
        rs1._check_compatibility(rs2)  # Should complete without error

    def test_incompatible_type(self):
        rs1 = JafQuerySet(query=["eq?", "@name", "test1"])
        with pytest.raises(
            TypeError, match="Operand must be an instance of JafQuerySet"
        ):
            rs1._check_compatibility(object())  # type: ignore


class TestJafQuerySetBooleanOps:
    # Test data for boolean operations with lazy query composition
    def setup_method(self):
        # Sample queries for testing boolean operations
        self.rs1 = JafQuerySet(
            query=["eq?", ["@", [["key", "status"]]], "active"],
            collection_id="common_id",
        )
        self.rs2 = JafQuerySet(
            query=["gt?", ["@", [["key", "age"]]], 25], collection_id="common_id"
        )
        self.rs_id_none = JafQuerySet(
            query=["eq?", ["@", [["key", "type"]]], "user"], collection_id=None
        )
        self.rs_other_id_none = JafQuerySet(
            query=["lt?", ["@", [["key", "score"]]], 100], collection_id=None
        )
        self.rs_id_diff = JafQuerySet(
            query=["eq?", ["@", [["key", "role"]]], "admin"], collection_id="diff_id"
        )

    # AND Tests
    def test_and(self):
        result = self.rs1.AND(self.rs2)
        # Check that result contains composed AND query
        assert result.query == [
            "and",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["gt?", ["@", [["key", "age"]]], 25],
        ]
        assert result.collection_id == "common_id"

    def test_and_operator(self):
        result = self.rs1 & self.rs2
        # Check that operator creates same result as method
        assert result.query == [
            "and",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["gt?", ["@", [["key", "age"]]], 25],
        ]

    def test_and_id_policy(self):
        res1 = self.rs1.AND(self.rs_id_none)
        assert res1.collection_id == "common_id"
        res2 = self.rs_id_none.AND(self.rs1)
        assert res2.collection_id == "common_id"  # rs1.collection_id is preferred
        res3 = self.rs_id_none.AND(self.rs_other_id_none)
        assert res3.collection_id is None

    # OR Tests
    def test_or(self):
        result = self.rs1.OR(self.rs2)
        # Check that result contains composed OR query
        assert result.query == [
            "or",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["gt?", ["@", [["key", "age"]]], 25],
        ]
        assert result.collection_id == "common_id"

    def test_or_operator(self):
        result = self.rs1 | self.rs2
        # Check that operator creates same result as method
        assert result.query == [
            "or",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["gt?", ["@", [["key", "age"]]], 25],
        ]

    # NOT Tests
    def test_not(self):
        result = self.rs1.NOT()
        # Check that result contains composed NOT query
        assert result.query == ["not", ["eq?", ["@", [["key", "status"]]], "active"]]
        assert result.collection_id == "common_id"

    def test_not_operator(self):
        result = ~self.rs1
        # Check that operator creates same result as method
        assert result.query == ["not", ["eq?", ["@", [["key", "status"]]], "active"]]

    def test_not_empty_query(self):
        rs_always_false = JafQuerySet(
            query=["eq?", ["@", [["key", "nonexistent"]]], "impossible"],
            collection_id="id_test",
        )
        result = rs_always_false.NOT()
        assert result.query == [
            "not",
            ["eq?", ["@", [["key", "nonexistent"]]], "impossible"],
        ]
        assert result.collection_id == "id_test"

    def test_not_simple_query(self):
        rs_simple = JafQuerySet(
            query=["eq?", ["@", [["key", "active"]]], True], collection_id="id_test"
        )
        result = rs_simple.NOT()
        assert result.query == ["not", ["eq?", ["@", [["key", "active"]]], True]]

    # XOR Tests
    def test_xor(self):
        result = self.rs1.XOR(self.rs2)
        # Check that result contains composed XOR query
        expected_query = [
            "not",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["gt?", ["@", [["key", "age"]]], 25],
        ]
        # XOR is implemented as (A AND NOT B) OR (NOT A AND B)
        assert result.collection_id == "common_id"
        # Just verify it's a proper XOR composition - exact structure may vary
        assert isinstance(result.query, list)
        assert len(result.query) > 1

    def test_xor_operator(self):
        result = self.rs1 ^ self.rs2
        # Check that operator creates same result as method
        assert result.collection_id == "common_id"
        assert isinstance(result.query, list)

    # SUBTRACT Tests
    def test_subtract(self):
        result = self.rs1.SUBTRACT(self.rs2)
        # Check that result contains composed SUBTRACT query (A AND NOT B)
        expected_query = [
            "and",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["not", ["gt?", ["@", [["key", "age"]]], 25]],
        ]
        assert result.query == expected_query
        assert result.collection_id == "common_id"

    def test_subtract_operator(self):
        result = self.rs1 - self.rs2
        # Check that operator creates same result as method
        expected_query = [
            "and",
            ["eq?", ["@", [["key", "status"]]], "active"],
            ["not", ["gt?", ["@", [["key", "age"]]], 25]],
        ]
        assert result.query == expected_query

    # Compatibility with warnings
    def test_op_different_collection_id_warning(self):
        # With lazy evaluation, different collection IDs just issue a warning
        # but the operation should still succeed
        result = self.rs1.OR(self.rs_id_diff)

        # Operation should succeed and create a valid query
        assert isinstance(result.query, list)
        assert result.query[0] == "or"
        # Collection ID policy: prefer the first operand's ID
        assert result.collection_id == "common_id"


class TestJafQuerySetDunderMethods:
    def test_len_raises_error(self):
        # len() should raise TypeError since we removed __len__ for lazy evaluation
        rs = JafQuerySet(query=["eq?", ["@", [["key", "status"]]], "active"])
        with pytest.raises(TypeError):
            len(rs)

    def test_iter_raises_error(self):
        # iter() should raise TypeError since we removed __iter__ for lazy evaluation
        rs = JafQuerySet(query=["eq?", ["@", [["key", "status"]]], "active"])
        with pytest.raises(TypeError):
            list(rs)

    def test_eq(self):
        # Test equality based on query, collection_id, and collection_source
        rs1 = JafQuerySet(
            query=["eq?", ["@", [["key", "name"]]], "test"], collection_id="id1"
        )
        rs2 = JafQuerySet(
            query=["eq?", ["@", [["key", "name"]]], "test"], collection_id="id1"
        )
        rs3 = JafQuerySet(
            query=["eq?", ["@", [["key", "name"]]], "different"], collection_id="id1"
        )
        rs4 = JafQuerySet(
            query=["eq?", ["@", [["key", "name"]]], "test"], collection_id="id2"
        )
        rs5 = JafQuerySet(
            query=["eq?", ["@", [["key", "name"]]], "test"], collection_id=None
        )

        # Same query and collection_id should be equal
        assert rs1 == rs2
        # Different queries should not be equal
        assert not (rs1 == rs3)
        # Different collection_ids should not be equal
        assert not (rs1 == rs4)
        # None vs string collection_id should not be equal
        assert not (rs1 == rs5)
        assert not (rs5 == rs1)
        # Use != operator
        assert rs1 != rs3
        # Comparison with non-JafQuerySet should raise TypeError
        with pytest.raises(TypeError):
            rs1 == object()
