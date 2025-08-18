import pytest
import time
from webants.libs.result import Field, Result

class TestField:
    def test_field_initialization(self):
        """测试Field类初始化"""
        value = "test_value"
        field = Field(value=value)
        assert field.value == value
        assert field.source is None
        assert field.extractor is None
        assert isinstance(field.timestamp, float)
        assert field.metadata == {}

    def test_field_with_metadata(self):
        """测试带元数据的Field"""
        metadata = {"key": "value"}
        field = Field(value=1, metadata=metadata)
        assert field.metadata == metadata

    def test_field_validation(self):
        """测试Field值验证"""
        with pytest.raises(ValueError):
            Field(value=None)

    def test_to_dict(self):
        """测试Field转字典"""
        field = Field(value="data", source="url", extractor="parser")
        field_dict = field.model_dump()
        assert field_dict["value"] == "data"
        assert field_dict["source"] == "url"
        assert field_dict["extractor"] == "parser"
        assert isinstance(field_dict["timestamp"], float)

class TestResult:
    @pytest.fixture
    def sample_result(self):
        """测试用的Result实例"""
        fields = {
            "title": Field(value="Test Title"),
            "content": Field(value="Test Content")
        }
        return Result(
            spider="test_spider",
            fields=fields,
            url="http://example.com",
            mediatype="text/html",
            title="Page Title",
            crawl_time=time.time()
        )

    def test_result_initialization(self, sample_result):
        """测试Result类初始化"""
        assert sample_result.spider == "test_spider"
        assert len(sample_result.fields) == 2
        assert sample_result.url == "http://example.com"
        assert sample_result.mediatype == "text/html"
        assert sample_result.title == "Page Title"
        assert isinstance(sample_result.crawl_time, float)

    def test_result_with_non_field_values(self):
        """测试使用非Field值初始化"""
        result = Result(
            spider="test_spider",
            fields={"title": "Test Title", "content": "Test Content"},
            url="http://example.com"
        )
        assert isinstance(result.fields["title"], Field)
        assert result.fields["title"].value == "Test Title"
        assert result.fields["title"].source == "http://example.com"
        assert result.fields["title"].extractor == "test_spider.parse"

    def test_add_field(self, sample_result):
        """测试添加字段"""
        sample_result.add_field("new_field", Field(value="new_value"))
        assert "new_field" in sample_result.fields
        assert sample_result.fields["new_field"].value == "new_value"

        sample_result.add_field("auto_field", "auto_value")
        assert isinstance(sample_result.fields["auto_field"], Field)
        assert sample_result.fields["auto_field"].value == "auto_value"

    def test_get_field(self, sample_result):
        """测试获取字段"""
        field = sample_result.get_field("title")
        assert field.value == "Test Title"
        assert sample_result.get_field("nonexistent") is None

    def test_get_value(self, sample_result):
        """测试获取字段值"""
        assert sample_result.get_value("title") == "Test Title"
        assert sample_result.get_value("nonexistent") is None

    def test_validation(self):
        """测试验证逻辑"""
        result = Result(
            spider="test_spider",
            fields={"invalid": None},  # 这会触发错误
            url="http://example.com"
        )
        assert not result.is_valid()
        assert len(result.get_errors()) == 1
        assert "Invalid field 'invalid'" in result.get_errors()[0]

    def test_to_dict(self, sample_result):
        """测试转字典方法"""
        result_dict = sample_result.to_dict()
        assert result_dict["spider"] == "test_spider"
        assert result_dict["url"] == "http://example.com"
        assert len(result_dict["fields"]) == 2
        assert "title" in result_dict["fields"]
        assert result_dict["fields"]["title"]["value"] == "Test Title"
        assert isinstance(result_dict["crawl_time"], float)
        assert result_dict["errors"] == []
        assert result_dict["warnings"] == []

    def test_repr(self, sample_result):
        """测试字符串表示"""
        assert str(sample_result) == "<Result http://example.com [Page Title]>"

    def test_save_method(self, sample_result):
        """测试保存方法(空实现)"""
        # 只是验证方法存在且可调用
        assert sample_result.save() is None