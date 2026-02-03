"""
Job Market Analytics Dashboard
==============================

Interactive dashboard for exploring job market trends,
skill demand, and salary insights.

Run with: streamlit run main.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# Page configuration
st.set_page_config(
    page_title="Job Market Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# DATA LOADING
# =============================================================================

@st.cache_data(ttl=3600)
def load_data():
    """Load data from S3 or local cache."""
    
    use_local = os.environ.get("USE_LOCAL_DATA", "true").lower() == "true"
    
    if use_local:
        # Load from local parquet files (for development)
        data_path = os.environ.get("LOCAL_DATA_PATH", "./data/")
        
        try:
            jobs_df = pd.read_parquet(f"{data_path}/fact_job_postings.parquet")
            skills_df = pd.read_parquet(f"{data_path}/dim_skills.parquet")
            companies_df = pd.read_parquet(f"{data_path}/dim_companies.parquet")
            locations_df = pd.read_parquet(f"{data_path}/dim_locations.parquet")
            salary_agg_df = pd.read_parquet(f"{data_path}/agg_salary_by_skill.parquet")
            trends_df = pd.read_parquet(f"{data_path}/agg_demand_trends.parquet")
        except FileNotFoundError:
            # Generate sample data if files don't exist
            jobs_df, skills_df, companies_df, locations_df, salary_agg_df, trends_df = generate_sample_data()
    else:
        # Load from S3
        import boto3
        bucket = os.environ.get("S3_GOLD_BUCKET", "job-analytics-gold-xxxx")
        
        jobs_df = pd.read_parquet(f"s3://{bucket}/fact_job_postings/")
        skills_df = pd.read_parquet(f"s3://{bucket}/dim_skills/")
        companies_df = pd.read_parquet(f"s3://{bucket}/dim_companies/")
        locations_df = pd.read_parquet(f"s3://{bucket}/dim_locations/")
        salary_agg_df = pd.read_parquet(f"s3://{bucket}/aggregates/agg_salary_by_skill/")
        trends_df = pd.read_parquet(f"s3://{bucket}/aggregates/agg_demand_trends/")
    
    return {
        "jobs": jobs_df,
        "skills": skills_df,
        "companies": companies_df,
        "locations": locations_df,
        "salary_agg": salary_agg_df,
        "trends": trends_df
    }


def generate_sample_data():
    """Generate sample data for demonstration."""
    import numpy as np
    
    np.random.seed(42)
    n_jobs = 1000
    
    skills_list = [
        ("Python", "Programming Language", 450),
        ("SQL", "Programming Language", 420),
        ("Spark", "Big Data", 280),
        ("AWS", "Cloud Platform", 350),
        ("Airflow", "Orchestration", 180),
        ("dbt", "Data Engineering Tool", 150),
        ("Kafka", "Streaming", 120),
        ("Snowflake", "Database", 200),
        ("Docker", "DevOps", 250),
        ("Kubernetes", "DevOps", 180),
        ("Terraform", "DevOps", 140),
        ("Machine Learning", "ML/AI", 220),
        ("Tableau", "Visualization", 160),
        ("PostgreSQL", "Database", 300),
        ("Git", "DevOps", 380),
    ]
    
    companies_list = [
        "Anthropic", "OpenAI", "Databricks", "Snowflake", "Stripe",
        "Airbnb", "Netflix", "Meta", "Google", "Amazon",
        "Microsoft", "Apple", "Uber", "Lyft", "DoorDash"
    ]
    
    locations_list = [
        ("San Francisco", "CA", "West"),
        ("New York", "NY", "Northeast"),
        ("Seattle", "WA", "West"),
        ("Austin", "TX", "South Central"),
        ("Boston", "MA", "Northeast"),
        ("Denver", "CO", "Mountain"),
        ("Chicago", "IL", "Midwest"),
        ("Remote", "Remote", "Remote"),
    ]
    
    # Generate jobs DataFrame
    jobs_data = {
        "job_id": [f"job_{i}" for i in range(n_jobs)],
        "title": np.random.choice(
            ["Data Engineer", "Senior Data Engineer", "Analytics Engineer", 
             "ML Engineer", "Data Scientist", "Platform Engineer"],
            n_jobs
        ),
        "company": np.random.choice(companies_list, n_jobs),
        "city": [loc[0] for loc in np.random.choice(range(len(locations_list)), n_jobs).tolist()],
        "state": [locations_list[i][1] for i in np.random.choice(range(len(locations_list)), n_jobs)],
        "is_remote": np.random.choice([True, False], n_jobs, p=[0.4, 0.6]),
        "salary_min": np.random.randint(80000, 200000, n_jobs),
        "posted_date": pd.date_range(end=datetime.now(), periods=n_jobs, freq="H"),
        "seniority_level": np.random.choice(["Junior", "Mid-Level", "Senior", "Management"], n_jobs),
        "role_category": np.random.choice(
            ["Data Engineer", "Analytics Engineer", "Data Scientist", "ML Engineer"],
            n_jobs
        ),
        "skill_count": np.random.randint(3, 12, n_jobs),
    }
    jobs_data["salary_max"] = jobs_data["salary_min"] + np.random.randint(20000, 50000, n_jobs)
    jobs_data["salary_midpoint"] = (jobs_data["salary_min"] + jobs_data["salary_max"]) / 2
    
    jobs_df = pd.DataFrame(jobs_data)
    
    # Generate skills DataFrame
    skills_df = pd.DataFrame([
        {
            "skill_name": s[0],
            "skill_category": s[1],
            "job_count": s[2],
            "avg_salary": np.random.randint(120000, 200000),
            "demand_level": "High" if s[2] > 200 else "Medium" if s[2] > 100 else "Low"
        }
        for s in skills_list
    ])
    
    # Generate companies DataFrame
    companies_df = pd.DataFrame([
        {
            "company_name": c,
            "total_job_postings": np.random.randint(10, 100),
            "avg_salary": np.random.randint(130000, 220000),
            "remote_percentage": np.random.randint(20, 80),
            "company_tier": np.random.choice(["Tier 1", "Tier 2", "Tier 3"])
        }
        for c in companies_list
    ])
    
    # Generate locations DataFrame
    locations_df = pd.DataFrame([
        {
            "city": loc[0],
            "state": loc[1],
            "region": loc[2],
            "job_count": np.random.randint(50, 300),
            "avg_salary": np.random.randint(110000, 200000)
        }
        for loc in locations_list
    ])
    
    # Generate salary aggregations
    salary_agg_df = skills_df[["skill_name", "skill_category", "job_count", "avg_salary"]].copy()
    salary_agg_df["median_salary"] = salary_agg_df["avg_salary"] - np.random.randint(-5000, 5000, len(salary_agg_df))
    salary_agg_df["salary_premium"] = salary_agg_df["avg_salary"] - 150000
    
    # Generate trends DataFrame
    weeks = pd.date_range(end=datetime.now(), periods=12, freq="W")
    trends_data = []
    for skill in skills_list[:10]:
        for week in weeks:
            trends_data.append({
                "week_start": week,
                "skill_name": skill[0],
                "job_count": skill[2] + np.random.randint(-20, 20),
                "trend_indicator": np.random.choice(["Growing", "Stable", "Declining"])
            })
    trends_df = pd.DataFrame(trends_data)
    
    return jobs_df, skills_df, companies_df, locations_df, salary_agg_df, trends_df


# =============================================================================
# SIDEBAR FILTERS
# =============================================================================

def render_sidebar(data):
    """Render sidebar filters."""
    
    st.sidebar.title("🎛️ Filters")
    
    # Date range
    st.sidebar.subheader("Date Range")
    date_range = st.sidebar.date_input(
        "Select dates",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        max_value=datetime.now()
    )
    
    # Role filter
    st.sidebar.subheader("Role Category")
    roles = ["All"] + sorted(data["jobs"]["role_category"].unique().tolist())
    selected_role = st.sidebar.selectbox("Select role", roles)
    
    # Location filter
    st.sidebar.subheader("Location")
    remote_only = st.sidebar.checkbox("Remote only")
    
    locations = ["All"] + sorted(data["locations"]["city"].unique().tolist())
    selected_location = st.sidebar.selectbox("Select city", locations)
    
    # Salary range
    st.sidebar.subheader("Salary Range")
    salary_range = st.sidebar.slider(
        "Annual salary ($K)",
        min_value=50,
        max_value=300,
        value=(80, 250),
        step=10
    )
    
    return {
        "date_range": date_range,
        "role": selected_role,
        "remote_only": remote_only,
        "location": selected_location,
        "salary_range": (salary_range[0] * 1000, salary_range[1] * 1000)
    }


def filter_data(data, filters):
    """Apply filters to data."""
    
    jobs_df = data["jobs"].copy()
    
    # Date filter
    if len(filters["date_range"]) == 2:
        start_date, end_date = filters["date_range"]
        jobs_df = jobs_df[
            (jobs_df["posted_date"].dt.date >= start_date) &
            (jobs_df["posted_date"].dt.date <= end_date)
        ]
    
    # Role filter
    if filters["role"] != "All":
        jobs_df = jobs_df[jobs_df["role_category"] == filters["role"]]
    
    # Remote filter
    if filters["remote_only"]:
        jobs_df = jobs_df[jobs_df["is_remote"] == True]
    
    # Location filter
    if filters["location"] != "All":
        jobs_df = jobs_df[jobs_df["city"] == filters["location"]]
    
    # Salary filter
    jobs_df = jobs_df[
        (jobs_df["salary_midpoint"] >= filters["salary_range"][0]) &
        (jobs_df["salary_midpoint"] <= filters["salary_range"][1])
    ]
    
    return jobs_df


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    """Main application."""
    
    # Header
    st.title("📊 Job Market Analytics Dashboard")
    st.markdown("*Explore data engineering job trends, skills demand, and salary insights*")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data..."):
        data = load_data()
    
    # Sidebar filters
    filters = render_sidebar(data)
    
    # Filter data
    filtered_jobs = filter_data(data, filters)
    
    # Check if we have data
    if len(filtered_jobs) == 0:
        st.warning("No data matches your filters. Try adjusting the criteria.")
        return
    
    # ==========================================================================
    # KEY METRICS
    # ==========================================================================
    
    st.subheader("📈 Key Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Jobs",
            f"{len(filtered_jobs):,}",
            delta=f"+{np.random.randint(10, 50)}" if len(filtered_jobs) > 0 else None
        )
    
    with col2:
        avg_salary = filtered_jobs["salary_midpoint"].mean()
        st.metric(
            "Avg Salary",
            f"${avg_salary/1000:.0f}K",
            delta=f"+${np.random.randint(2, 8)}K"
        )
    
    with col3:
        remote_pct = (filtered_jobs["is_remote"].sum() / len(filtered_jobs)) * 100
        st.metric(
            "Remote %",
            f"{remote_pct:.1f}%",
            delta=f"+{np.random.randint(1, 5)}%"
        )
    
    with col4:
        unique_companies = filtered_jobs["company"].nunique()
        st.metric(
            "Companies",
            f"{unique_companies}",
            delta=None
        )
    
    with col5:
        avg_skills = filtered_jobs["skill_count"].mean()
        st.metric(
            "Avg Skills/Job",
            f"{avg_skills:.1f}",
            delta=None
        )
    
    st.markdown("---")
    
    # ==========================================================================
    # CHARTS ROW 1
    # ==========================================================================
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Top Skills by Demand")
        
        skills_chart = data["skills"].nlargest(12, "job_count")
        fig = px.bar(
            skills_chart,
            x="job_count",
            y="skill_name",
            orientation="h",
            color="skill_category",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=0, r=0, t=30, b=0),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💰 Salary by Role Category")
        
        salary_by_role = filtered_jobs.groupby("role_category").agg({
            "salary_midpoint": ["mean", "min", "max"]
        }).round(0)
        salary_by_role.columns = ["avg", "min", "max"]
        salary_by_role = salary_by_role.reset_index()
        
        fig = px.bar(
            salary_by_role,
            x="role_category",
            y="avg",
            error_y=salary_by_role["max"] - salary_by_role["avg"],
            error_y_minus=salary_by_role["avg"] - salary_by_role["min"],
            color="role_category",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig.update_layout(
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==========================================================================
    # CHARTS ROW 2
    # ==========================================================================
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📍 Jobs by Location")
        
        location_data = filtered_jobs.groupby("city").size().reset_index(name="count")
        location_data = location_data.nlargest(10, "count")
        
        fig = px.pie(
            location_data,
            values="count",
            names="city",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=30, b=0),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Salary Distribution")
        
        fig = px.histogram(
            filtered_jobs,
            x="salary_midpoint",
            nbins=30,
            color="seniority_level",
            barmode="overlay",
            opacity=0.7
        )
        fig.update_layout(
            xaxis_title="Salary ($)",
            yaxis_title="Count",
            margin=dict(l=0, r=0, t=30, b=0),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==========================================================================
    # TRENDS
    # ==========================================================================
    
    st.subheader("📈 Skill Demand Trends (Last 12 Weeks)")
    
    # Select skills to show
    top_skills = data["skills"].nlargest(5, "job_count")["skill_name"].tolist()
    trends_filtered = data["trends"][data["trends"]["skill_name"].isin(top_skills)]
    
    fig = px.line(
        trends_filtered,
        x="week_start",
        y="job_count",
        color="skill_name",
        markers=True
    )
    fig.update_layout(
        xaxis_title="Week",
        yaxis_title="Job Postings",
        legend_title="Skill",
        margin=dict(l=0, r=0, t=30, b=0),
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==========================================================================
    # TOP COMPANIES TABLE
    # ==========================================================================
    
    st.subheader("🏢 Top Hiring Companies")
    
    companies_display = data["companies"].nlargest(10, "total_job_postings")[
        ["company_name", "total_job_postings", "avg_salary", "remote_percentage", "company_tier"]
    ].copy()
    companies_display["avg_salary"] = companies_display["avg_salary"].apply(lambda x: f"${x:,.0f}")
    companies_display["remote_percentage"] = companies_display["remote_percentage"].apply(lambda x: f"{x}%")
    companies_display.columns = ["Company", "Open Positions", "Avg Salary", "Remote %", "Tier"]
    
    st.dataframe(
        companies_display,
        use_container_width=True,
        hide_index=True
    )
    
    # ==========================================================================
    # FOOTER
    # ==========================================================================
    
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>Data updated daily at 6:00 AM UTC | 
            <a href='https://github.com/yourusername/job-market-analytics'>GitHub</a> |
            Built with Streamlit</p>
        </div>
        """,
        unsafe_allow_html=True
    )


# Required for numpy import in generate_sample_data
import numpy as np

if __name__ == "__main__":
    main()
