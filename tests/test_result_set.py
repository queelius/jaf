import pytest
from jaf.result_set import JafResultSet, JafResultSetError

class TestJafResultSetInit:
    def test_valid_creation(self):
        rs = JafResultSet({1, 2}, 5, "id1")
        assert rs.indices == {1, 2}
        assert rs.collection_size == 5
        assert rs.collection_id == "id1"

    def test_valid_creation_empty_indices(self):
        rs = JafResultSet([], 3)
        assert rs.indices == set()
        assert rs.collection_size == 3
        assert rs.collection_id is None

    def test_valid_creation_collection_size_zero_empty_indices(self):
        rs = JafResultSet([], 0)
        assert rs.indices == set()
        assert rs.collection_size == 0

    def test_invalid_collection_size_negative(self):
        with pytest.raises(ValueError, match="collection_size must be a non-negative integer"):
            JafResultSet({1}, -1)

    def test_invalid_collection_size_type(self):
        with pytest.raises(ValueError, match="collection_size must be a non-negative integer"):
            JafResultSet({1}, "abc") # type: ignore

    def test_invalid_indices_out_of_bounds_upper(self):
        # Adjusted match to be more general for the "Found: {indices_set}" part
        with pytest.raises(ValueError, match=r"All indices must be integers within the range \[0, 2\]. Found invalid index: 3"):
            JafResultSet({0, 3}, 3)
            
    def test_invalid_indices_out_of_bounds_negative(self):
        with pytest.raises(ValueError, match=r"All indices must be integers within the range \[0, 2\]. Found invalid index: -1"):
            JafResultSet({-1, 1}, 3)

    def test_invalid_indices_type(self):
        # The representation of the set with mixed types might vary, so match the core message.
        # The actual error message will list the problematic set, e.g., Found: {1, 'a'}
        with pytest.raises(ValueError, match=r"All indices must be integers within the range \[0, 2\]. Found invalid index: a"):
            JafResultSet({"a", 1}, 3) # type: ignore

    def test_invalid_indices_collection_size_zero_non_empty_indices(self):
        # This will fail the 0 <= i < self.collection_size (0 <= 0 < 0 is false)
        with pytest.raises(ValueError, match=r"Indices must be empty if collection_size is 0."):
            JafResultSet({0}, 0)

class TestJafResultSetSerialization:
    def test_to_dict(self):
        rs = JafResultSet({2, 0, 1}, 5, "id_test")
        expected = {
            "indices": [0, 1, 2], # to_dict sorts indices
            "collection_size": 5,
            "collection_id": "id_test",
            "collection_source": None,
            "query": None
        }
        assert rs.to_dict() == expected

    def test_to_dict_no_collection_id(self):
        rs = JafResultSet(set(), 3)
        expected = {
            "indices": [],
            "collection_size": 3,
            "collection_id": None,
            "collection_source": None,
            "query": None
        }
        assert rs.to_dict() == expected

    def test_from_dict_valid(self):
        data = {
            "indices": [0, 1, 2],
            "collection_size": 5,
            "collection_id": "id_test"
        }
        rs = JafResultSet.from_dict(data)
        assert rs.indices == {0, 1, 2}
        assert rs.collection_size == 5
        assert rs.collection_id == "id_test"
        assert rs.collection_source is None

    def test_from_dict_indices_as_set(self):
        data = {
            "indices": {0, 1, 2}, 
            "collection_size": 5,
            "collection_id": "id_test"
        }
        rs = JafResultSet.from_dict(data)
        assert rs.indices == {0, 1, 2}

    def test_from_dict_missing_indices(self):
        with pytest.raises(ValueError, match="JafResultSet.from_dict: Missing required key in input data: 'indices'"):
            JafResultSet.from_dict({"collection_size": 5})

    def test_from_dict_missing_collection_size(self):
        with pytest.raises(ValueError, match="JafResultSet.from_dict: Missing required key in input data: 'collection_size'"):
            JafResultSet.from_dict({"indices": [1]})

    def test_from_dict_invalid_indices_type(self):
        with pytest.raises(ValueError, match="JafResultSet.from_dict: Type error in input data: 'indices' must be a list or set."):
            JafResultSet.from_dict({"indices": "1,2,3", "collection_size": 5})

    def test_from_dict_invalid_collection_size_type(self):
        with pytest.raises(ValueError, match="JafResultSet.from_dict: 'collection_size' must be an integer."):
            JafResultSet.from_dict({"indices": [1], "collection_size": "5"})
            
    def test_from_dict_indices_out_of_bounds(self):
        data = {"indices": [0, 4], "collection_size": 3}
        # This calls __init__, so the error message comes from there.
        with pytest.raises(ValueError, match=r"All indices must be integers within the range \[0, 2\]. Found invalid index:"):
            JafResultSet.from_dict(data)


class TestJafResultSetCompatibility:
    def test_compatible(self):
        rs1 = JafResultSet({1}, 5, "id1")
        rs2 = JafResultSet({2}, 5, "id1")
        rs1._check_compatibility(rs2) 

    def test_compatible_one_id_none(self):
        rs1 = JafResultSet({1}, 5, "id1")
        rs2 = JafResultSet({2}, 5, None)
        rs1._check_compatibility(rs2) 

    def test_compatible_both_ids_none(self):
        rs1 = JafResultSet({1}, 5)
        rs2 = JafResultSet({2}, 5)
        rs1._check_compatibility(rs2) 

    def test_incompatible_collection_size(self):
        rs1 = JafResultSet({1}, 5)
        rs2 = JafResultSet({2}, 6)
        with pytest.raises(JafResultSetError, match="Collection sizes do not match: 5 != 6"):
            rs1._check_compatibility(rs2)

    def test_incompatible_collection_id(self):
        rs1 = JafResultSet({1}, 5, "id1")
        rs2 = JafResultSet({2}, 5, "id2")
        with pytest.raises(JafResultSetError, match="Collection IDs do not match: 'id1' != 'id2'"):
            rs1._check_compatibility(rs2)

    def test_incompatible_type(self):
        rs1 = JafResultSet({1}, 5)
        with pytest.raises(TypeError, match="Operand must be an instance of JafResultSet"):
            rs1._check_compatibility(object()) # type: ignore

class TestJafResultSetBooleanOps:
    # Define rs1 and rs2 inside setup_method or as class attributes if they don't change
    # For simplicity here, defining them as they were, assuming they are immutable for tests
    rs1 = JafResultSet({0, 1, 2}, 5, "common_id")
    rs2 = JafResultSet({2, 3, 4}, 5, "common_id")
    rs_id_none = JafResultSet({1,2}, 5, None)
    rs_other_id_none = JafResultSet({2,3}, 5, None)
    rs_id_diff = JafResultSet({1,2}, 5, "diff_id")


    # AND Tests
    def test_and(self):
        result = self.rs1.AND(self.rs2)
        assert result.indices == {2}
        assert result.collection_size == 5
        assert result.collection_id == "common_id"

    def test_and_operator(self):
        result = self.rs1 & self.rs2
        assert result.indices == {2}

    def test_and_id_policy(self):
        res1 = self.rs1.AND(self.rs_id_none) 
        assert res1.collection_id == "common_id"
        res2 = self.rs_id_none.AND(self.rs1) 
        assert res2.collection_id == "common_id" # rs1.collection_id is preferred
        res3 = self.rs_id_none.AND(self.rs_other_id_none) 
        assert res3.collection_id is None


    # OR Tests
    def test_or(self):
        result = self.rs1.OR(self.rs2)
        assert result.indices == {0, 1, 2, 3, 4}
        assert result.collection_id == "common_id"

    def test_or_operator(self):
        result = self.rs1 | self.rs2
        assert result.indices == {0, 1, 2, 3, 4}

    # NOT Tests
    def test_not(self):
        result = self.rs1.NOT()
        assert result.indices == {3, 4}
        assert result.collection_size == 5
        assert result.collection_id == "common_id"

    def test_not_operator(self):
        result = ~self.rs1
        assert result.indices == {3, 4}

    def test_not_empty_set(self):
        rs_empty = JafResultSet(set(), 3, "id_empty")
        result = rs_empty.NOT()
        assert result.indices == {0, 1, 2}
        assert result.collection_id == "id_empty"

    def test_not_full_set(self):
        rs_full = JafResultSet({0,1,2}, 3, "id_full")
        result = rs_full.NOT()
        assert result.indices == set()

    # XOR Tests
    def test_xor(self):
        result = self.rs1.XOR(self.rs2)
        assert result.indices == {0, 1, 3, 4}
        assert result.collection_id == "common_id"

    def test_xor_operator(self):
        result = self.rs1 ^ self.rs2
        assert result.indices == {0, 1, 3, 4}

    # SUBTRACT Tests
    def test_subtract(self):
        result = self.rs1.SUBTRACT(self.rs2) 
        assert result.indices == {0, 1}
        assert result.collection_id == "common_id"

    def test_subtract_operator(self):
        result = self.rs1 - self.rs2
        assert result.indices == {0, 1}

    # Incompatibility
    def test_op_incompatible_size(self):
        rs_diff_size = JafResultSet({1}, 6, "common_id")
        with pytest.raises(JafResultSetError, match="Collection sizes do not match"):
            self.rs1.AND(rs_diff_size)

    def test_op_incompatible_id(self):
        with pytest.raises(JafResultSetError, match="Collection IDs do not match"):
            self.rs1.OR(self.rs_id_diff)

class TestJafResultSetDunderMethods:
    def test_len(self):
        rs = JafResultSet({0, 1, 2}, 5)
        assert len(rs) == 3
        rs_empty = JafResultSet(set(), 5)
        assert len(rs_empty) == 0

    def test_iter(self):
        rs = JafResultSet({2, 0, 1}, 3)
        assert list(rs) == [0, 1, 2] 

    def test_eq(self):
        rs1 = JafResultSet({0, 1}, 2, "id1")
        rs2 = JafResultSet({0, 1}, 2, "id1")
        rs3 = JafResultSet({0}, 2, "id1")     
        rs4 = JafResultSet({0, 1}, 3, "id1")  
        rs5 = JafResultSet({0, 1}, 2, "id2")  
        rs6 = JafResultSet({0, 1}, 2, None)   

        assert rs1 == rs2
        assert not (rs1 == rs3)
        assert not (rs1 == rs4)
        assert not (rs1 == rs5)
        assert not (rs1 == rs6)
        assert not (rs6 == rs1)
        assert rs1 != rs3 
        assert rs1 != object()
        assert not (rs1 == object())