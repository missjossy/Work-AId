#!/usr/bin/env python3
"""
Run updated feature importance analysis with new demographic attributes
Includes: Employment, Marital Status, Education Level
"""

import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    print("="*80)
    print("UPDATED FEATURE IMPORTANCE ANALYSIS")
    print("Now including: Employment, Marital Status, Education Level")
    print("="*80)
    
    print("\n1. Running Persona Characteristics Analysis...")
    print("   - Feature importance ranking with new attributes")
    print("   - Statistical significance testing")
    print("   - Impact visualization")
    
    try:
        from persona_characteristics_analysis import main as run_persona_analysis
        run_persona_analysis()
    except Exception as e:
        print(f"Error in persona characteristics analysis: {e}")
    
    print("\n" + "="*80)
    print("\n2. Running Demographic Analysis...")
    print("   - Individual demographic distributions")
    print("   - Combination analysis with new attributes")
    print("   - Persona insights")
    
    try:
        from demographic_analysis import main as run_demographic_analysis
        run_demographic_analysis()
    except Exception as e:
        print(f"Error in demographic analysis: {e}")
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("New attributes included: Employment, Marital Status, Education Level")
    print("="*80)

if __name__ == "__main__":
    main()

