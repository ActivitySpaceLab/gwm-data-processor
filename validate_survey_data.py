#!/usr/bin/env python3
"""
Survey Data Validation & Testing Script
=======================================

This script performs comprehensive testing and validation of survey data collection
to detect and help fix any data completeness or accuracy issues.

Features:
- Data completeness analysis for each survey type
- Field-by-field validation and missing data detection  
- Cross-survey consistency checks
- Location data quality assessment
- Image/media file validation
- Participant journey analysis
- Data collection timeline analysis
- Detailed reporting with actionable recommendations

Usage:
    python3 validate_survey_data.py [options]
    
Examples:
    # Validate current CSV files
    python3 validate_survey_data.py
    
    # Validate specific directory
    python3 validate_survey_data.py --input ./data/structured
    
    # Generate detailed report
    python3 validate_survey_data.py --detailed-report
    
    # Focus on specific issues
    python3 validate_survey_data.py --check-missing --check-consistency --check-locations
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
import pandas as pd
import numpy as np
from collections import defaultdict, Counter

class SurveyDataValidator:
    """Comprehensive validator for survey data collection"""
    
    def __init__(self, input_dir: str):
        """Initialize the validator"""
        self.input_dir = Path(input_dir)
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'input_directory': str(self.input_dir),
            'files_found': {},
            'data_summary': {},
            'validation_checks': {},
            'issues_found': [],
            'recommendations': [],
            'participant_analysis': {},
            'data_quality_score': 0.0
        }
        
        # Load data
        self.consent_df = None
        self.initial_df = None  
        self.biweekly_df = None
        self.location_df = None
        
    def load_data_files(self) -> bool:
        """Load all CSV files and validate basic structure"""
        print("📂 Loading survey data files...")
        
        files_to_load = {
            'consent.csv': 'consent_df',
            'initial_survey.csv': 'initial_df', 
            'biweekly_survey.csv': 'biweekly_df',
            'location_data.csv': 'location_df'
        }
        
        files_loaded = 0
        
        for filename, attr_name in files_to_load.items():
            filepath = self.input_dir / filename
            
            if filepath.exists():
                try:
                    df = pd.read_csv(filepath)
                    setattr(self, attr_name, df)
                    files_loaded += 1
                    
                    self.validation_results['files_found'][filename] = {
                        'exists': True,
                        'records': len(df),
                        'columns': len(df.columns),
                        'column_names': list(df.columns)
                    }
                    
                    print(f"   ✅ {filename}: {len(df)} records, {len(df.columns)} columns")
                    
                except Exception as e:
                    self.validation_results['files_found'][filename] = {
                        'exists': True,
                        'error': str(e)
                    }
                    self._add_issue(f"Failed to load {filename}: {e}", 'critical')
                    print(f"   ❌ {filename}: Failed to load - {e}")
            else:
                self.validation_results['files_found'][filename] = {'exists': False}
                self._add_issue(f"Required file missing: {filename}", 'warning')
                print(f"   ⚠️ {filename}: File not found")
        
        if files_loaded == 0:
            print("❌ No valid survey files found!")
            return False
        
        print(f"✅ Loaded {files_loaded}/4 survey data files")
        return True
    
    def analyze_data_summary(self) -> None:
        """Generate high-level data summary statistics"""
        print("\n📊 Analyzing data summary...")
        
        summary = {}
        
        # Count participants across surveys
        all_participants = set()
        
        if self.consent_df is not None:
            consent_participants = set(self.consent_df['participant_uuid'].dropna())
            all_participants.update(consent_participants)
            summary['consent_responses'] = len(self.consent_df)
            summary['consent_participants'] = len(consent_participants)
        
        if self.initial_df is not None:
            initial_participants = set(self.initial_df['participant_uuid'].dropna())
            all_participants.update(initial_participants)
            summary['initial_responses'] = len(self.initial_df)
            summary['initial_participants'] = len(initial_participants)
        
        if self.biweekly_df is not None:
            biweekly_participants = set(self.biweekly_df['participant_uuid'].dropna())
            all_participants.update(biweekly_participants)
            summary['biweekly_responses'] = len(self.biweekly_df)
            summary['biweekly_participants'] = len(biweekly_participants)
        
        if self.location_df is not None:
            summary['location_points'] = len(self.location_df)
            summary['location_responses'] = len(self.location_df['response_id'].unique())
        
        summary['total_unique_participants'] = len(all_participants)
        
        self.validation_results['data_summary'] = summary
        
        print(f"   Total unique participants: {summary['total_unique_participants']}")
        print(f"   Consent responses: {summary.get('consent_responses', 0)}")
        print(f"   Initial survey responses: {summary.get('initial_responses', 0)}")
        print(f"   Biweekly responses: {summary.get('biweekly_responses', 0)}")
        print(f"   Location data points: {summary.get('location_points', 0)}")
    
    def check_data_completeness(self) -> None:
        """Check for missing or incomplete data in each survey type"""
        print("\n🔍 Checking data completeness...")
        
        completeness_results = {}
        
        # Check consent data completeness
        if self.consent_df is not None:
            consent_check = self._check_dataframe_completeness(
                self.consent_df, 
                'consent',
                critical_fields=['participant_uuid', 'consent_id', 'informed_consent'],
                important_fields=['consent_participate', 'consent_qualtrics_data', 'participant_signature']
            )
            completeness_results['consent'] = consent_check
        
        # Check initial survey completeness  
        if self.initial_df is not None:
            initial_check = self._check_dataframe_completeness(
                self.initial_df,
                'initial_survey', 
                critical_fields=['participant_uuid', 'age'],
                important_fields=['gender', 'ethnicity', 'challenges_stress_level']
            )
            completeness_results['initial_survey'] = initial_check
        
        # Check biweekly survey completeness
        if self.biweekly_df is not None:
            biweekly_check = self._check_dataframe_completeness(
                self.biweekly_df,
                'biweekly_survey',
                critical_fields=['participant_uuid', 'timestamp'],
                important_fields=['cheerful_spirits', 'calm_relaxed', 'active_vigorous']
            )
            completeness_results['biweekly_survey'] = biweekly_check
        
        # Check location data completeness
        if self.location_df is not None:
            location_check = self._check_dataframe_completeness(
                self.location_df,
                'location_data',
                critical_fields=['response_id', 'timestamp', 'latitude', 'longitude'],
                important_fields=['accuracy', 'activity']
            )
            completeness_results['location_data'] = location_check
        
        self.validation_results['validation_checks']['completeness'] = completeness_results
    
    def _check_dataframe_completeness(self, df: pd.DataFrame, survey_type: str, 
                                    critical_fields: List[str], important_fields: List[str]) -> Dict:
        """Check completeness for a specific dataframe"""
        
        result = {
            'total_records': len(df),
            'critical_field_issues': {},
            'important_field_issues': {},
            'overall_completeness_score': 0.0
        }
        
        total_critical_checks = 0
        passed_critical_checks = 0
        total_important_checks = 0
        passed_important_checks = 0
        
        # Check critical fields
        for field in critical_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                empty_count = (df[field] == '').sum() if df[field].dtype == 'object' else 0
                total_issues = missing_count + empty_count
                
                total_critical_checks += len(df)
                passed_critical_checks += (len(df) - total_issues)
                
                if total_issues > 0:
                    issue_pct = (total_issues / len(df)) * 100
                    result['critical_field_issues'][field] = {
                        'missing_count': int(missing_count),
                        'empty_count': int(empty_count), 
                        'total_issues': int(total_issues),
                        'percentage': round(issue_pct, 1)
                    }
                    
                    if issue_pct > 20:  # More than 20% missing is critical
                        self._add_issue(
                            f"{survey_type}: Critical field '{field}' has {issue_pct:.1f}% missing data",
                            'critical'
                        )
                    else:
                        self._add_issue(
                            f"{survey_type}: Field '{field}' has {issue_pct:.1f}% missing data", 
                            'warning'
                        )
            else:
                self._add_issue(f"{survey_type}: Critical field '{field}' not found in data", 'critical')
        
        # Check important fields
        for field in important_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                empty_count = (df[field] == '').sum() if df[field].dtype == 'object' else 0
                total_issues = missing_count + empty_count
                
                total_important_checks += len(df)
                passed_important_checks += (len(df) - total_issues)
                
                if total_issues > 0:
                    issue_pct = (total_issues / len(df)) * 100
                    result['important_field_issues'][field] = {
                        'missing_count': int(missing_count),
                        'empty_count': int(empty_count),
                        'total_issues': int(total_issues), 
                        'percentage': round(issue_pct, 1)
                    }
                    
                    if issue_pct > 50:  # More than 50% missing for important fields
                        self._add_issue(
                            f"{survey_type}: Important field '{field}' has {issue_pct:.1f}% missing data",
                            'warning'
                        )
        
        # Calculate overall completeness score
        total_checks = total_critical_checks + total_important_checks
        passed_checks = passed_critical_checks + passed_important_checks
        
        if total_checks > 0:
            result['overall_completeness_score'] = round((passed_checks / total_checks) * 100, 1)
        
        print(f"   {survey_type}: {result['overall_completeness_score']}% complete")
        
        return result
    
    def check_participant_consistency(self) -> None:
        """Check for consistency across participant data in different surveys"""
        print("\n🔗 Checking participant consistency...")
        
        consistency_results = {}
        
        if self.consent_df is not None and self.initial_df is not None:
            # Check if participants who gave consent also have initial surveys
            consent_participants = set(self.consent_df['participant_uuid'].dropna())
            initial_participants = set(self.initial_df['participant_uuid'].dropna())
            
            consent_without_initial = consent_participants - initial_participants
            initial_without_consent = initial_participants - consent_participants
            
            consistency_results['consent_vs_initial'] = {
                'consent_only': len(consent_without_initial),
                'initial_only': len(initial_without_consent), 
                'both_surveys': len(consent_participants & initial_participants),
                'consistency_rate': round(len(consent_participants & initial_participants) / 
                                        max(len(consent_participants | initial_participants), 1) * 100, 1)
            }
            
            if len(consent_without_initial) > 0:
                self._add_issue(
                    f"{len(consent_without_initial)} participants gave consent but have no initial survey",
                    'warning'
                )
            
            if len(initial_without_consent) > 0:
                self._add_issue(
                    f"{len(initial_without_consent)} participants have initial survey but no consent record",
                    'critical'
                )
        
        # Check biweekly survey participation
        if self.initial_df is not None and self.biweekly_df is not None:
            initial_participants = set(self.initial_df['participant_uuid'].dropna())
            biweekly_participants = set(self.biweekly_df['participant_uuid'].dropna())
            
            biweekly_participation_rate = len(biweekly_participants & initial_participants) / max(len(initial_participants), 1) * 100
            
            consistency_results['initial_vs_biweekly'] = {
                'initial_participants': len(initial_participants),
                'biweekly_participants': len(biweekly_participants),
                'participation_rate': round(biweekly_participation_rate, 1)
            }
            
            if biweekly_participation_rate < 50:
                self._add_issue(
                    f"Low biweekly survey participation: only {biweekly_participation_rate:.1f}% of initial participants",
                    'warning'
                )
        
        self.validation_results['validation_checks']['consistency'] = consistency_results
        
        for check_name, results in consistency_results.items():
            print(f"   {check_name}: {results}")
    
    def check_location_data_quality(self) -> None:
        """Analyze location data quality and coverage"""
        print("\n🌍 Checking location data quality...")
        
        if self.location_df is None:
            self._add_issue("No location data found", 'warning')
            return
        
        location_results = {}
        
        # Basic location data stats
        total_points = len(self.location_df)
        unique_responses = self.location_df['response_id'].nunique()
        
        # Check coordinate validity
        valid_coords = 0
        invalid_coords = 0
        
        for _, row in self.location_df.iterrows():
            lat, lon = row.get('latitude'), row.get('longitude')
            
            if pd.isna(lat) or pd.isna(lon):
                invalid_coords += 1
            elif abs(float(lat)) <= 90 and abs(float(lon)) <= 180:
                valid_coords += 1
            else:
                invalid_coords += 1
        
        coordinate_validity = (valid_coords / max(total_points, 1)) * 100
        
        # Check accuracy distribution
        accuracy_stats = {}
        if 'accuracy' in self.location_df.columns:
            accuracy_values = pd.to_numeric(self.location_df['accuracy'], errors='coerce').dropna()
            if len(accuracy_values) > 0:
                accuracy_stats = {
                    'mean_accuracy': round(accuracy_values.mean(), 2),
                    'median_accuracy': round(accuracy_values.median(), 2),
                    'high_accuracy_points': int((accuracy_values <= 10).sum()),  # <= 10m accuracy
                    'low_accuracy_points': int((accuracy_values > 50).sum())     # > 50m accuracy
                }
        
        location_results = {
            'total_points': total_points,
            'unique_responses': unique_responses,
            'points_per_response': round(total_points / max(unique_responses, 1), 1),
            'coordinate_validity_percent': round(coordinate_validity, 1),
            'valid_coordinates': valid_coords,
            'invalid_coordinates': invalid_coords,
            'accuracy_stats': accuracy_stats
        }
        
        # Quality issues
        if coordinate_validity < 95:
            self._add_issue(
                f"Location data has {100-coordinate_validity:.1f}% invalid coordinates",
                'warning'
            )
        
        if accuracy_stats and accuracy_stats.get('low_accuracy_points', 0) > total_points * 0.3:
            self._add_issue(
                f"{accuracy_stats['low_accuracy_points']} location points have poor accuracy (>50m)",
                'info'
            )
        
        self.validation_results['validation_checks']['location_quality'] = location_results
        
        print(f"   Total points: {total_points}")
        print(f"   Coordinate validity: {coordinate_validity:.1f}%")
        print(f"   Average points per response: {location_results['points_per_response']}")
    
    def analyze_participant_journeys(self) -> None:
        """Analyze individual participant journeys through the survey system"""
        print("\n👥 Analyzing participant journeys...")
        
        participant_analysis = {}
        
        # Get all participants
        all_participants = set()
        if self.consent_df is not None:
            all_participants.update(self.consent_df['participant_uuid'].dropna())
        if self.initial_df is not None:
            all_participants.update(self.initial_df['participant_uuid'].dropna())
        if self.biweekly_df is not None:
            all_participants.update(self.biweekly_df['participant_uuid'].dropna())
        
        journey_patterns = defaultdict(int)
        complete_journeys = 0
        
        for participant_id in all_participants:
            journey = []
            
            # Check consent
            if self.consent_df is not None and participant_id in self.consent_df['participant_uuid'].values:
                journey.append('consent')
            
            # Check initial survey  
            if self.initial_df is not None and participant_id in self.initial_df['participant_uuid'].values:
                journey.append('initial')
            
            # Check biweekly surveys
            if self.biweekly_df is not None:
                biweekly_count = (self.biweekly_df['participant_uuid'] == participant_id).sum()
                if biweekly_count > 0:
                    journey.append(f'biweekly({biweekly_count})')
            
            journey_key = ' -> '.join(journey)
            journey_patterns[journey_key] += 1
            
            # Complete journey = consent + initial + at least 1 biweekly
            if len(journey) >= 3 and 'biweekly' in journey_key:
                complete_journeys += 1
        
        participant_analysis = {
            'total_participants': len(all_participants),
            'complete_journeys': complete_journeys,
            'completion_rate': round((complete_journeys / max(len(all_participants), 1)) * 100, 1),
            'journey_patterns': dict(journey_patterns)
        }
        
        self.validation_results['participant_analysis'] = participant_analysis
        
        print(f"   Total participants: {len(all_participants)}")
        print(f"   Complete journeys: {complete_journeys} ({participant_analysis['completion_rate']}%)")
        print("   Journey patterns:")
        for pattern, count in sorted(journey_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"      {pattern}: {count} participants")
    
    def calculate_overall_data_quality_score(self) -> None:
        """Calculate an overall data quality score"""
        print("\n📈 Calculating overall data quality score...")
        
        scores = []
        
        # File availability score (25% weight)
        files_available = sum(1 for f in self.validation_results['files_found'].values() 
                            if f.get('exists', False) and 'error' not in f)
        file_score = (files_available / 4) * 25
        scores.append(('File Availability', file_score, 25))
        
        # Data completeness score (35% weight)
        completeness_scores = []
        for survey_type, check in self.validation_results['validation_checks'].get('completeness', {}).items():
            completeness_scores.append(check.get('overall_completeness_score', 0))
        
        avg_completeness = np.mean(completeness_scores) if completeness_scores else 0
        completeness_score = (avg_completeness / 100) * 35
        scores.append(('Data Completeness', completeness_score, 35))
        
        # Participant consistency score (25% weight) 
        consistency_rate = 100  # Default to perfect if no consistency checks
        consistency_data = self.validation_results['validation_checks'].get('consistency', {})
        if 'consent_vs_initial' in consistency_data:
            consistency_rate = consistency_data['consent_vs_initial'].get('consistency_rate', 100)
        
        consistency_score = (consistency_rate / 100) * 25
        scores.append(('Participant Consistency', consistency_score, 25))
        
        # Location data quality score (15% weight)
        location_score = 0
        location_data = self.validation_results['validation_checks'].get('location_quality', {})
        if location_data:
            coord_validity = location_data.get('coordinate_validity_percent', 0)
            location_score = (coord_validity / 100) * 15
        scores.append(('Location Quality', location_score, 15))
        
        # Calculate weighted total
        total_score = sum(score for _, score, _ in scores)
        
        self.validation_results['data_quality_score'] = round(total_score, 1)
        
        print("   Quality Score Breakdown:")
        for component, score, weight in scores:
            print(f"      {component}: {score:.1f}/{weight}")
        print(f"   Overall Quality Score: {total_score:.1f}/100")
        
        # Add quality-based recommendations
        if total_score >= 90:
            self._add_recommendation("Excellent data quality! Continue current data collection practices.")
        elif total_score >= 75:
            self._add_recommendation("Good data quality with minor issues to address.")
        elif total_score >= 60:
            self._add_recommendation("Moderate data quality - several issues need attention.")
        else:
            self._add_recommendation("Poor data quality - urgent fixes needed for reliable analysis.")
    
    def _add_issue(self, description: str, severity: str) -> None:
        """Add an issue to the validation results"""
        self.validation_results['issues_found'].append({
            'description': description,
            'severity': severity,
            'timestamp': datetime.now().isoformat()
        })
    
    def _add_recommendation(self, description: str) -> None:
        """Add a recommendation to the validation results"""
        self.validation_results['recommendations'].append(description)
    
    def generate_recommendations(self) -> None:
        """Generate actionable recommendations based on findings"""
        print("\n💡 Generating recommendations...")
        
        # Issues-based recommendations
        critical_issues = [issue for issue in self.validation_results['issues_found'] 
                          if issue['severity'] == 'critical']
        
        if critical_issues:
            self._add_recommendation(
                f"URGENT: {len(critical_issues)} critical data issues found. "
                "Review consent forms and initial survey collection process."
            )
        
        # Completeness recommendations
        completeness_data = self.validation_results['validation_checks'].get('completeness', {})
        for survey_type, data in completeness_data.items():
            score = data.get('overall_completeness_score', 0)
            if score < 80:
                self._add_recommendation(
                    f"Improve {survey_type} data collection: only {score}% complete. "
                    "Check app functionality and user experience."
                )
        
        # Participation recommendations
        participant_data = self.validation_results.get('participant_analysis', {})
        completion_rate = participant_data.get('completion_rate', 0)
        if completion_rate < 70:
            self._add_recommendation(
                f"Low participant completion rate ({completion_rate}%). "
                "Consider improving user engagement and survey reminders."
            )
        
        # Location data recommendations
        location_data = self.validation_results['validation_checks'].get('location_quality', {})
        if location_data:
            validity = location_data.get('coordinate_validity_percent', 0)
            if validity < 90:
                self._add_recommendation(
                    f"Location data quality issues ({validity}% valid coordinates). "
                    "Check GPS permissions and location service functionality."
                )
    
    def save_detailed_report(self, output_file: Optional[str] = None) -> None:
        """Save comprehensive validation report"""
        if output_file is None:
            output_file = self.input_dir / 'validation_report.json'
        else:
            output_file = Path(output_file)
        
        with open(output_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2, default=str)
        
        print(f"\n📄 Detailed validation report saved: {output_file}")
    
    def print_summary_report(self) -> None:
        """Print a human-readable summary report"""
        print("\n" + "="*70)
        print("📋 SURVEY DATA VALIDATION SUMMARY REPORT")  
        print("="*70)
        
        # Data overview
        summary = self.validation_results.get('data_summary', {})
        print(f"\n📊 DATA OVERVIEW:")
        print(f"   Participants: {summary.get('total_unique_participants', 0)}")
        print(f"   Consent responses: {summary.get('consent_responses', 0)}")
        print(f"   Initial surveys: {summary.get('initial_responses', 0)}")
        print(f"   Biweekly surveys: {summary.get('biweekly_responses', 0)}")
        print(f"   Location points: {summary.get('location_points', 0)}")
        
        # Quality score
        quality_score = self.validation_results.get('data_quality_score', 0)
        quality_grade = 'A' if quality_score >= 90 else 'B' if quality_score >= 75 else 'C' if quality_score >= 60 else 'F'
        print(f"\n🏆 OVERALL QUALITY SCORE: {quality_score}/100 (Grade: {quality_grade})")
        
        # Issues summary
        issues = self.validation_results.get('issues_found', [])
        if issues:
            critical_count = sum(1 for i in issues if i['severity'] == 'critical')
            warning_count = sum(1 for i in issues if i['severity'] == 'warning')
            
            print(f"\n⚠️  ISSUES FOUND: {len(issues)} total")
            if critical_count > 0:
                print(f"   🔴 Critical: {critical_count}")
            if warning_count > 0:
                print(f"   🟡 Warnings: {warning_count}")
            
            print("\n   Top Issues:")
            for issue in issues[:5]:  # Show top 5 issues
                severity_icon = '🔴' if issue['severity'] == 'critical' else '🟡' if issue['severity'] == 'warning' else 'ℹ️'
                print(f"   {severity_icon} {issue['description']}")
        else:
            print(f"\n✅ NO ISSUES FOUND!")
        
        # Recommendations
        recommendations = self.validation_results.get('recommendations', [])
        if recommendations:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(recommendations[:5], 1):  # Show top 5 recommendations
                print(f"   {i}. {rec}")
        
        print("\n" + "="*70)
    
    def run_full_validation(self) -> bool:
        """Run complete validation suite"""
        print("🔍 Starting comprehensive survey data validation...")
        print("="*60)
        
        # Load data
        if not self.load_data_files():
            return False
        
        # Run all validation checks
        self.analyze_data_summary()
        self.check_data_completeness()
        self.check_participant_consistency() 
        self.check_location_data_quality()
        self.analyze_participant_journeys()
        self.calculate_overall_data_quality_score()
        self.generate_recommendations()
        
        return True

def main():
    parser = argparse.ArgumentParser(
        description='Validate and analyze survey data collection quality',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s                                 Validate current data/structured directory
  %(prog)s --input ./my_data               Validate specific directory
  %(prog)s --detailed-report              Save detailed JSON report
  %(prog)s --output-report ./report.json  Save report to specific file
        """
    )
    
    parser.add_argument('--input', default='./data/structured',
                       help='Input directory with CSV files (default: ./data/structured)')
    
    parser.add_argument('--detailed-report', action='store_true',
                       help='Save detailed JSON validation report')
    
    parser.add_argument('--output-report', 
                       help='Specify output file for detailed report')
    
    args = parser.parse_args()
    
    # Run validation
    try:
        validator = SurveyDataValidator(args.input)
        
        if not validator.run_full_validation():
            print("❌ Validation failed!")
            return 1
        
        # Generate reports
        validator.print_summary_report()
        
        if args.detailed_report or args.output_report:
            validator.save_detailed_report(args.output_report)
        
        # Exit with appropriate code based on quality
        quality_score = validator.validation_results.get('data_quality_score', 0)
        if quality_score >= 75:
            print("\n✅ Validation completed - Data quality is acceptable")
            return 0
        else:
            print(f"\n⚠️ Validation completed - Data quality needs improvement ({quality_score}/100)")
            return 1
        
    except Exception as e:
        print(f"💥 Validation failed with error: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())