"""
Configuration validation module for production safety.

Step C: Production Hardening
- Schema validation for all config values
- Range checks for numeric parameters
- Dependency validation (e.g., agent_key requires user_address)
- Warning for risky configurations
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    ERROR = auto()    # Blocks startup
    WARNING = auto()  # Logs warning but allows startup
    INFO = auto()     # Informational only


@dataclass
class ValidationIssue:
    """A single validation issue."""
    field: str
    message: str
    severity: ValidationSeverity
    value: Any = None
    suggestion: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of config validation."""
    valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    
    def has_errors(self) -> bool:
        return any(i.severity == ValidationSeverity.ERROR for i in self.issues)
    
    def has_warnings(self) -> bool:
        return any(i.severity == ValidationSeverity.WARNING for i in self.issues)
    
    def get_errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]
    
    def get_warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]


class ConfigValidator:
    """
    Validates Settings configuration for production safety.
    
    Checks:
    - Required fields are present
    - Numeric values are within safe ranges
    - Dependencies between fields
    - Risky configurations
    """

    # Range definitions: (min, max, default_if_missing)
    NUMERIC_RANGES: Dict[str, Tuple[float, float, Optional[float]]] = {
        "investment_usd": (10.0, 1_000_000.0, None),
        "leverage": (1.0, 50.0, 10.0),
        "grids": (3, 200, 30),
        "base_spacing_pct": (0.0001, 0.1, 0.0006),
        "max_spacing_pct": (0.001, 0.5, 0.005),
        "trailing_pct": (0.0, 0.1, 0.001),
        "loop_interval": (0.5, 60.0, 2.0),
        "rest_audit_interval": (10.0, 3600.0, 300.0),
        "max_drawdown_pct": (0.01, 0.5, 0.12),
        "max_unrealized_dd_pct": (0.01, 1.0, 0.15),
        "funding_bleed_pct": (0.001, 0.1, 0.02),
        "pnl_daily_stop": (-10_000_000.0, 0.0, -1_000_000.0),
        "pnl_daily_take": (0.0, 10_000_000.0, 1_000_000.0),
        "ws_stale_after": (5.0, 300.0, 20.0),
        "http_timeout": (5.0, 120.0, 30.0),
        "coalesce_ms": (0, 10000, 500),
        "api_error_threshold": (1, 100, 5),
    }

    # Required string fields
    REQUIRED_STRINGS: List[str] = [
        "base_url",
        "dex",
    ]

    # Conditional requirements
    CONDITIONAL_REQUIREMENTS: List[Tuple[str, str, str]] = [
        # (if_field, then_required, message)
        ("agent_key", "user_address", "agent_key requires user_address to be set"),
    ]

    def __init__(self) -> None:
        self._custom_validators: List[callable] = []

    def register_validator(self, validator: callable) -> None:
        """Register a custom validation function."""
        self._custom_validators.append(validator)

    def validate(self, cfg) -> ValidationResult:
        """
        Validate a Settings object.
        
        Args:
            cfg: Settings instance to validate
            
        Returns:
            ValidationResult with all issues found
        """
        issues: List[ValidationIssue] = []
        
        # Check required strings
        issues.extend(self._validate_required_strings(cfg))
        
        # Check numeric ranges
        issues.extend(self._validate_numeric_ranges(cfg))
        
        # Check conditional requirements
        issues.extend(self._validate_conditional(cfg))
        
        # Check coins configuration
        issues.extend(self._validate_coins(cfg))
        
        # Check for risky configurations
        issues.extend(self._check_risky_configs(cfg))
        
        # Run custom validators
        for validator in self._custom_validators:
            try:
                custom_issues = validator(cfg)
                if custom_issues:
                    issues.extend(custom_issues)
            except Exception as e:
                logger.warning(f"Custom validator error: {e}")
        
        has_errors = any(i.severity == ValidationSeverity.ERROR for i in issues)
        return ValidationResult(valid=not has_errors, issues=issues)

    def _validate_required_strings(self, cfg) -> List[ValidationIssue]:
        """Validate required string fields."""
        issues = []
        for field_name in self.REQUIRED_STRINGS:
            value = getattr(cfg, field_name, None)
            if not value or (isinstance(value, str) and not value.strip()):
                issues.append(ValidationIssue(
                    field=field_name,
                    message=f"Required field '{field_name}' is missing or empty",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                ))
        return issues

    def _validate_numeric_ranges(self, cfg) -> List[ValidationIssue]:
        """Validate numeric fields are within acceptable ranges."""
        issues = []
        for field_name, (min_val, max_val, default) in self.NUMERIC_RANGES.items():
            value = getattr(cfg, field_name, None)
            if value is None:
                if default is None:
                    issues.append(ValidationIssue(
                        field=field_name,
                        message=f"Required numeric field '{field_name}' is missing",
                        severity=ValidationSeverity.ERROR,
                    ))
                continue
            
            try:
                num_value = float(value)
                if num_value < min_val:
                    issues.append(ValidationIssue(
                        field=field_name,
                        message=f"'{field_name}' value {num_value} is below minimum {min_val}",
                        severity=ValidationSeverity.ERROR,
                        value=num_value,
                        suggestion=f"Set to at least {min_val}",
                    ))
                elif num_value > max_val:
                    issues.append(ValidationIssue(
                        field=field_name,
                        message=f"'{field_name}' value {num_value} is above maximum {max_val}",
                        severity=ValidationSeverity.ERROR,
                        value=num_value,
                        suggestion=f"Set to at most {max_val}",
                    ))
            except (TypeError, ValueError):
                issues.append(ValidationIssue(
                    field=field_name,
                    message=f"'{field_name}' has invalid numeric value: {value}",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                ))
        return issues

    def _validate_conditional(self, cfg) -> List[ValidationIssue]:
        """Validate conditional field requirements."""
        issues = []
        for if_field, then_required, message in self.CONDITIONAL_REQUIREMENTS:
            if_value = getattr(cfg, if_field, None)
            then_value = getattr(cfg, then_required, None)
            
            if if_value and not then_value:
                issues.append(ValidationIssue(
                    field=then_required,
                    message=message,
                    severity=ValidationSeverity.ERROR,
                    suggestion=f"Set '{then_required}' when using '{if_field}'",
                ))
        return issues

    def _validate_coins(self, cfg) -> List[ValidationIssue]:
        """Validate coins configuration."""
        issues = []
        coins = getattr(cfg, "coins", None)
        
        if not coins:
            issues.append(ValidationIssue(
                field="coins",
                message="No coins configured",
                severity=ValidationSeverity.ERROR,
                suggestion="Set HL_COINS or HL_COIN environment variable",
            ))
            return issues
        
        if not isinstance(coins, list):
            issues.append(ValidationIssue(
                field="coins",
                message="Coins must be a list",
                severity=ValidationSeverity.ERROR,
                value=coins,
            ))
            return issues
        
        for coin in coins:
            if ":" not in coin:
                issues.append(ValidationIssue(
                    field="coins",
                    message=f"Invalid coin format '{coin}', expected 'dex:symbol' (e.g., 'xyz:TSLA')",
                    severity=ValidationSeverity.ERROR,
                    value=coin,
                ))
        
        if len(coins) > 10:
            issues.append(ValidationIssue(
                field="coins",
                message=f"Trading {len(coins)} coins may strain resources",
                severity=ValidationSeverity.WARNING,
                value=len(coins),
                suggestion="Consider trading fewer coins or increasing resources",
            ))
        
        return issues

    def _check_risky_configs(self, cfg) -> List[ValidationIssue]:
        """Check for risky but valid configurations."""
        issues = []
        
        # High leverage warning
        leverage = getattr(cfg, "leverage", 10.0)
        if leverage > 20:
            issues.append(ValidationIssue(
                field="leverage",
                message=f"High leverage ({leverage}x) increases liquidation risk",
                severity=ValidationSeverity.WARNING,
                value=leverage,
                suggestion="Consider using lower leverage for safety",
            ))
        
        # Low grid count warning
        grids = getattr(cfg, "grids", 30)
        if grids < 10:
            issues.append(ValidationIssue(
                field="grids",
                message=f"Low grid count ({grids}) may result in fewer trading opportunities",
                severity=ValidationSeverity.WARNING,
                value=grids,
            ))
        
        # High spacing warning
        max_spacing = getattr(cfg, "max_spacing_pct", 0.005)
        if max_spacing > 0.02:
            issues.append(ValidationIssue(
                field="max_spacing_pct",
                message=f"Wide grid spacing ({max_spacing:.1%}) may miss price movements",
                severity=ValidationSeverity.WARNING,
                value=max_spacing,
            ))
        
        # Tight drawdown limit
        max_dd = getattr(cfg, "max_drawdown_pct", 0.12)
        if max_dd < 0.05:
            issues.append(ValidationIssue(
                field="max_drawdown_pct",
                message=f"Tight drawdown limit ({max_dd:.1%}) may trigger frequent halts",
                severity=ValidationSeverity.WARNING,
                value=max_dd,
            ))
        
        # No authentication
        agent_key = getattr(cfg, "agent_key", None)
        private_key = getattr(cfg, "private_key", None)
        if not agent_key and not private_key:
            issues.append(ValidationIssue(
                field="agent_key",
                message="No authentication configured (agent_key or private_key)",
                severity=ValidationSeverity.ERROR,
                suggestion="Set HL_AGENT_KEY or HL_PRIVATE_KEY",
            ))
        
        # Large investment warning
        investment = getattr(cfg, "investment_usd", 200)
        if investment > 50000:
            issues.append(ValidationIssue(
                field="investment_usd",
                message=f"Large investment (${investment:,.0f}) - ensure risk limits are appropriate",
                severity=ValidationSeverity.WARNING,
                value=investment,
            ))
        
        return issues


def validate_config(cfg) -> ValidationResult:
    """
    Convenience function to validate config.
    
    Args:
        cfg: Settings instance to validate
        
    Returns:
        ValidationResult
    """
    validator = ConfigValidator()
    return validator.validate(cfg)


def validate_and_log(cfg, logger_instance=None) -> bool:
    """
    Validate config and log all issues.
    
    Args:
        cfg: Settings instance to validate
        logger_instance: Optional logger (uses module logger if not provided)
        
    Returns:
        True if config is valid (no errors), False otherwise
    """
    log = logger_instance or logger
    result = validate_config(cfg)
    
    for issue in result.get_errors():
        msg = f"CONFIG ERROR: {issue.message}"
        if issue.suggestion:
            msg += f" (suggestion: {issue.suggestion})"
        log.error(msg)
    
    for issue in result.get_warnings():
        msg = f"CONFIG WARNING: {issue.message}"
        if issue.suggestion:
            msg += f" (suggestion: {issue.suggestion})"
        log.warning(msg)
    
    if result.valid:
        log.info("Configuration validation passed")
    else:
        log.error(f"Configuration validation failed with {len(result.get_errors())} error(s)")
    
    return result.valid
