"""
Tests for pipeline/src/models.py — all V2 dataclasses.

Covers:
- Import surface (all 7 classes importable from pipeline.src.models)
- V1 Job fields remain intact
- V2 Job field additions
- Field types and defaults for every dataclass
- Sentinel values (defaults, None-ability)
- posted_date property alias
- ScoreDimension.pass_num (reserved word avoidance)
"""

import dataclasses
from typing import Optional, get_args, get_origin, Union

import pytest

from pipeline.src.models import (
    CareerPageConfig,
    Company,
    Feedback,
    Job,
    ProfileSnapshot,
    ProfileSuggestion,
    ScoreDimension,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _field_names(cls) -> list[str]:
    """Return the list of dataclass field names for *cls*."""
    return [f.name for f in dataclasses.fields(cls)]


def _field_map(cls) -> dict[str, dataclasses.Field]:
    return {f.name: f for f in dataclasses.fields(cls)}


def _is_optional(annotation) -> bool:
    """Return True if *annotation* is Optional[X] (i.e. Union[X, None])."""
    return get_origin(annotation) is Union and type(None) in get_args(annotation)


# ---------------------------------------------------------------------------
# Import smoke test
# ---------------------------------------------------------------------------


class TestImports:
    def test_all_classes_importable(self):
        """AC #5: all 7 classes importable from pipeline.src.models."""
        # If the import at the top of this module succeeded, this passes.
        for cls in (
            Job,
            Company,
            ScoreDimension,
            Feedback,
            ProfileSnapshot,
            CareerPageConfig,
            ProfileSuggestion,
        ):
            assert cls is not None

    def test_classes_are_dataclasses(self):
        for cls in (
            Job,
            Company,
            ScoreDimension,
            Feedback,
            ProfileSnapshot,
            CareerPageConfig,
            ProfileSuggestion,
        ):
            assert dataclasses.is_dataclass(cls), f"{cls.__name__} is not a dataclass"


# ---------------------------------------------------------------------------
# Job — V1 fields intact (AC #6)
# ---------------------------------------------------------------------------


V1_REQUIRED_FIELDS = ["title", "company", "url", "description", "source"]
V1_OPTIONAL_FIELDS = [
    "location",
    "salary_min",
    "salary_max",
    "posted_at",
    "match_score",
    "match_reasoning",
    "db_id",
    "raw",
]


class TestJobV1Fields:
    def test_v1_required_fields_present(self):
        fields = _field_names(Job)
        for name in V1_REQUIRED_FIELDS:
            assert name in fields, f"Missing V1 required field: {name}"

    def test_v1_optional_fields_present(self):
        fields = _field_names(Job)
        for name in V1_OPTIONAL_FIELDS:
            assert name in fields, f"Missing V1 optional field: {name}"

    def test_posted_date_property(self):
        job = Job(
            title="SWE",
            company="Acme",
            url="https://example.com/job/1",
            description="A job.",
            source="adzuna",
            source_type="api",
            posted_at="2026-03-01",
        )
        assert job.posted_date == "2026-03-01"

    def test_posted_date_none_when_posted_at_none(self):
        job = Job(
            title="SWE",
            company="Acme",
            url="https://example.com/job/2",
            description="A job.",
            source="adzuna",
            source_type="api",
        )
        assert job.posted_date is None

    def test_v1_optional_fields_default_to_none(self):
        job = Job(
            title="SWE",
            company="Acme",
            url="https://example.com/job/3",
            description="A job.",
            source="adzuna",
            source_type="api",
        )
        for name in V1_OPTIONAL_FIELDS:
            assert getattr(job, name) is None, f"Expected None for {name}"

    def test_raw_excluded_from_repr(self):
        job = Job(
            title="SWE",
            company="Acme",
            url="https://example.com/job/4",
            description="A job.",
            source="adzuna",
            source_type="api",
            raw={"key": "value"},
        )
        assert "raw" not in repr(job)


# ---------------------------------------------------------------------------
# Job — V2 fields (AC #1)
# ---------------------------------------------------------------------------


V2_JOB_FIELDS = [
    "source_type",
    "company_id",
    "ats_platform",
    "dedup_hash",
    "last_seen_at",
    "external_id",
]


class TestJobV2Fields:
    def test_v2_fields_present(self):
        fields = _field_names(Job)
        for name in V2_JOB_FIELDS:
            assert name in fields, f"Missing V2 field: {name}"

    def test_source_type_required(self):
        """source_type has no default — must be provided."""
        with pytest.raises(TypeError):
            Job(  # type: ignore[call-arg]
                title="SWE",
                company="Acme",
                url="https://example.com/job/5",
                description="A job.",
                source="adzuna",
                # source_type omitted
            )

    def test_v2_optional_fields_default_to_none(self):
        job = Job(
            title="SWE",
            company="Acme",
            url="https://example.com/job/6",
            description="A job.",
            source="adzuna",
            source_type="api",
        )
        for name in ["company_id", "ats_platform", "dedup_hash", "last_seen_at", "external_id"]:
            assert getattr(job, name) is None, f"Expected None for {name}"

    def test_source_type_roundtrip(self):
        for stype in ("api", "career_page", "ats_feed"):
            job = Job(
                title="SWE",
                company="Acme",
                url=f"https://example.com/{stype}",
                description="A job.",
                source="adzuna",
                source_type=stype,
            )
            assert job.source_type == stype

    def test_v2_fields_assignable(self):
        job = Job(
            title="SWE",
            company="Acme",
            url="https://example.com/job/7",
            description="A job.",
            source="adzuna",
            source_type="api",
        )
        job.company_id = 42
        job.ats_platform = "greenhouse"
        job.dedup_hash = "abc123"
        job.last_seen_at = "2026-03-20T12:00:00"
        job.external_id = "ext-999"
        assert job.company_id == 42
        assert job.ats_platform == "greenhouse"
        assert job.dedup_hash == "abc123"
        assert job.last_seen_at == "2026-03-20T12:00:00"
        assert job.external_id == "ext-999"


# ---------------------------------------------------------------------------
# Company (AC #2)
# ---------------------------------------------------------------------------


class TestCompany:
    EXPECTED_FIELDS = [
        "id",
        "name",
        "domain",
        "career_page_url",
        "ats_platform",
        "size_range",
        "industry",
        "funding_stage",
        "glassdoor_rating",
        "glassdoor_url",
        "tech_stack",
        "crunchbase_data",
        "enriched_at",
        "is_target",
        "created_at",
    ]

    def test_all_columns_present(self):
        fields = _field_names(Company)
        for name in self.EXPECTED_FIELDS:
            assert name in fields, f"Missing Company field: {name}"

    def test_name_required(self):
        c = Company(name="Acme Corp")
        assert c.name == "Acme Corp"

    def test_defaults(self):
        c = Company(name="Acme Corp")
        assert c.id is None
        assert c.domain is None
        assert c.ats_platform is None
        assert c.size_range is None
        assert c.industry is None
        assert c.funding_stage is None
        assert c.glassdoor_rating is None
        assert c.glassdoor_url is None
        assert c.tech_stack is None
        assert c.crunchbase_data is None
        assert c.enriched_at is None
        assert c.is_target == 0
        assert c.created_at is None

    def test_is_target_default_zero(self):
        c = Company(name="Acme Corp")
        assert c.is_target == 0

    def test_full_construction(self):
        c = Company(
            id=1,
            name="Acme Corp",
            domain="acme.com",
            career_page_url="https://acme.com/careers",
            ats_platform="greenhouse",
            size_range="51-200",
            industry="Software",
            funding_stage="series_b",
            glassdoor_rating=4.2,
            glassdoor_url="https://glassdoor.com/acme",
            tech_stack='["Python","React"]',
            crunchbase_data='{"founded":2018}',
            enriched_at="2026-03-20T00:00:00",
            is_target=1,
            created_at="2026-03-01T00:00:00",
        )
        assert c.id == 1
        assert c.glassdoor_rating == 4.2
        assert c.is_target == 1


# ---------------------------------------------------------------------------
# ScoreDimension (AC #3)
# ---------------------------------------------------------------------------


class TestScoreDimension:
    EXPECTED_FIELDS = [
        "id",
        "job_id",
        "pass_num",
        "role_fit",
        "skills_gap",
        "culture_signals",
        "growth_potential",
        "comp_alignment",
        "overall",
        "reasoning",
        "scored_at",
        "profile_hash",
    ]

    def test_all_columns_present(self):
        fields = _field_names(ScoreDimension)
        for name in self.EXPECTED_FIELDS:
            assert name in fields, f"Missing ScoreDimension field: {name}"

    def test_pass_not_a_field_name(self):
        """Verify reserved word avoidance: no field named 'pass'."""
        assert "pass" not in _field_names(ScoreDimension)

    def test_pass_num_field_exists(self):
        assert "pass_num" in _field_names(ScoreDimension)

    def test_minimal_construction(self):
        sd = ScoreDimension(job_id=1, pass_num=1, overall=75)
        assert sd.job_id == 1
        assert sd.pass_num == 1
        assert sd.overall == 75

    def test_optional_dimensions_default_none(self):
        sd = ScoreDimension(job_id=1, pass_num=1, overall=0)
        for dim in ("role_fit", "skills_gap", "culture_signals", "growth_potential", "comp_alignment"):
            assert getattr(sd, dim) is None, f"Expected None for {dim}"

    def test_full_pass2_construction(self):
        sd = ScoreDimension(
            id=10,
            job_id=5,
            pass_num=2,
            role_fit=85,
            skills_gap=70,
            culture_signals=60,
            growth_potential=80,
            comp_alignment=75,
            overall=77,
            reasoning='{"role_fit":"Good title match"}',
            scored_at="2026-03-20T10:00:00",
            profile_hash="sha256abc",
        )
        assert sd.pass_num == 2
        assert sd.overall == 77
        assert sd.role_fit == 85

    def test_pass_num_both_values(self):
        sd1 = ScoreDimension(job_id=1, pass_num=1, overall=0)
        sd2 = ScoreDimension(job_id=1, pass_num=2, overall=82)
        assert sd1.pass_num == 1
        assert sd2.pass_num == 2


# ---------------------------------------------------------------------------
# Feedback (AC #4)
# ---------------------------------------------------------------------------


class TestFeedback:
    EXPECTED_FIELDS = ["id", "job_id", "signal", "note", "created_at"]

    def test_all_columns_present(self):
        fields = _field_names(Feedback)
        for name in self.EXPECTED_FIELDS:
            assert name in fields, f"Missing Feedback field: {name}"

    def test_minimal_construction(self):
        fb = Feedback(job_id=3, signal="thumbs_up")
        assert fb.job_id == 3
        assert fb.signal == "thumbs_up"

    def test_defaults(self):
        fb = Feedback(job_id=3, signal="thumbs_down")
        assert fb.note is None
        assert fb.created_at is None
        assert fb.id is None

    def test_thumbs_up_and_down(self):
        up = Feedback(job_id=1, signal="thumbs_up")
        down = Feedback(job_id=2, signal="thumbs_down")
        assert up.signal == "thumbs_up"
        assert down.signal == "thumbs_down"

    def test_with_note(self):
        fb = Feedback(job_id=7, signal="thumbs_down", note="Too junior")
        assert fb.note == "Too junior"


# ---------------------------------------------------------------------------
# ProfileSnapshot (AC #4)
# ---------------------------------------------------------------------------


class TestProfileSnapshot:
    EXPECTED_FIELDS = ["id", "profile_yaml", "resume_hash", "extracted_skills", "created_at"]

    def test_all_columns_present(self):
        fields = _field_names(ProfileSnapshot)
        for name in self.EXPECTED_FIELDS:
            assert name in fields, f"Missing ProfileSnapshot field: {name}"

    def test_minimal_construction(self):
        ps = ProfileSnapshot(profile_yaml="title: SWE\n")
        assert ps.profile_yaml == "title: SWE\n"

    def test_defaults(self):
        ps = ProfileSnapshot(profile_yaml="title: SWE\n")
        assert ps.id is None
        assert ps.resume_hash is None
        assert ps.extracted_skills is None
        assert ps.created_at is None

    def test_full_construction(self):
        ps = ProfileSnapshot(
            id=2,
            profile_yaml="title: SWE\n",
            resume_hash="sha256xyz",
            extracted_skills='["Python","Go"]',
            created_at="2026-03-20T08:00:00",
        )
        assert ps.id == 2
        assert ps.resume_hash == "sha256xyz"


# ---------------------------------------------------------------------------
# CareerPageConfig (AC #4)
# ---------------------------------------------------------------------------


class TestCareerPageConfig:
    EXPECTED_FIELDS = [
        "id",
        "company_id",
        "url",
        "discovery_method",
        "scrape_strategy",
        "last_crawled_at",
        "status",
        "created_at",
    ]

    def test_all_columns_present(self):
        fields = _field_names(CareerPageConfig)
        for name in self.EXPECTED_FIELDS:
            assert name in fields, f"Missing CareerPageConfig field: {name}"

    def test_minimal_construction(self):
        cfg = CareerPageConfig(
            company_id=1,
            url="https://acme.com/careers",
            discovery_method="auto",
        )
        assert cfg.company_id == 1
        assert cfg.url == "https://acme.com/careers"
        assert cfg.discovery_method == "auto"

    def test_status_default(self):
        cfg = CareerPageConfig(
            company_id=1,
            url="https://acme.com/careers",
            discovery_method="manual",
        )
        assert cfg.status == "active"

    def test_defaults(self):
        cfg = CareerPageConfig(
            company_id=2,
            url="https://beta.com/careers",
            discovery_method="auto",
        )
        assert cfg.id is None
        assert cfg.scrape_strategy is None
        assert cfg.last_crawled_at is None
        assert cfg.created_at is None

    def test_status_values(self):
        for status in ("active", "broken", "disabled"):
            cfg = CareerPageConfig(
                company_id=1,
                url="https://x.com/careers",
                discovery_method="auto",
                status=status,
            )
            assert cfg.status == status

    def test_with_scrape_strategy(self):
        cfg = CareerPageConfig(
            company_id=3,
            url="https://gamma.com/careers",
            discovery_method="auto",
            scrape_strategy='{"selector":".job-listing"}',
        )
        assert cfg.scrape_strategy == '{"selector":".job-listing"}'


# ---------------------------------------------------------------------------
# ProfileSuggestion (AC #4)
# ---------------------------------------------------------------------------


class TestProfileSuggestion:
    EXPECTED_FIELDS = [
        "id",
        "suggestion_type",
        "description",
        "reasoning",
        "suggested_change",
        "status",
        "created_at",
        "resolved_at",
    ]

    def test_all_columns_present(self):
        fields = _field_names(ProfileSuggestion)
        for name in self.EXPECTED_FIELDS:
            assert name in fields, f"Missing ProfileSuggestion field: {name}"

    def test_minimal_construction(self):
        ps = ProfileSuggestion(
            suggestion_type="add_skill",
            description="Add ML to skills",
            reasoning="You liked 8 ML jobs.",
            suggested_change='{"skills": ["ML"]}',
        )
        assert ps.suggestion_type == "add_skill"

    def test_status_default(self):
        ps = ProfileSuggestion(
            suggestion_type="add_keyword",
            description="desc",
            reasoning="reason",
            suggested_change="{}",
        )
        assert ps.status == "pending"

    def test_defaults(self):
        ps = ProfileSuggestion(
            suggestion_type="remove_skill",
            description="desc",
            reasoning="reason",
            suggested_change="{}",
        )
        assert ps.id is None
        assert ps.created_at is None
        assert ps.resolved_at is None

    def test_status_values(self):
        for status in ("pending", "approved", "rejected"):
            ps = ProfileSuggestion(
                suggestion_type="adjust_weight",
                description="d",
                reasoning="r",
                suggested_change="{}",
                status=status,
            )
            assert ps.status == status

    def test_full_construction(self):
        ps = ProfileSuggestion(
            id=5,
            suggestion_type="add_skill",
            description="Add ML",
            reasoning="Pattern in feedback",
            suggested_change='{"add":["ML"]}',
            status="approved",
            created_at="2026-03-20T09:00:00",
            resolved_at="2026-03-20T10:00:00",
        )
        assert ps.id == 5
        assert ps.status == "approved"
        assert ps.resolved_at == "2026-03-20T10:00:00"
