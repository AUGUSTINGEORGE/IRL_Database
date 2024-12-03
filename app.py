import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_setup import Method, MethodTechnologyService, Task, Technology
from math import pi
import time

# Database setup
engine = create_engine('sqlite:///fuel_cell_database.db')
Session = sessionmaker(bind=engine)
session = Session()

st.title("Fuel Cell Modeling Monte Carlo Simulation")

# Sidebar to view data from the database
st.sidebar.header("Database Viewer")

@st.cache_data
def fetch_data():
    """Fetch data from the database."""
    tasks = session.query(Task).all()
    technologies = session.query(Technology).all()
    methods = session.query(Method).all()
    return tasks, technologies, methods

def group_methods_by_type(methods):
    """Organize methods by their types."""
    methods_by_type = {}
    for method in methods:
        method_type = method.method_type.strip() if method.method_type else "Uncategorized"
        if method_type not in methods_by_type:
            methods_by_type[method_type] = []
        methods_by_type[method_type].append(method)
    return methods_by_type

# Initialize session state
if "tasks" not in st.session_state:
    st.session_state.tasks, st.session_state.technologies, st.session_state.all_methods = fetch_data()
    st.session_state.methods_by_type = group_methods_by_type(st.session_state.all_methods)

# Refresh logic
if st.sidebar.button("Refresh Data"):
    st.sidebar.write("Refreshing...")
    time.sleep(1)
    st.session_state.tasks, st.session_state.technologies, st.session_state.all_methods = fetch_data()
    st.session_state.methods_by_type = group_methods_by_type(st.session_state.all_methods)
    st.sidebar.success("Data refreshed!")

# Debug mode toggle
if st.sidebar.checkbox("Enable Debug Mode"):
    st.sidebar.write("Debug mode enabled. Logs will appear here.")
    st.write("Session State:", st.session_state)

# Access data from session state
tasks = st.session_state.tasks
technologies = st.session_state.technologies
all_methods = st.session_state.all_methods
methods_by_type = st.session_state.methods_by_type

# Sidebar data views
if st.sidebar.checkbox("View All Methods"):
    st.sidebar.write("**All Methods**")
    for method in all_methods:
        st.sidebar.write(f"ID: {method.method_id}, Name: {method.name}, Type: {method.method_type}")

if st.sidebar.checkbox("View Methods by Task"):
    for task in tasks:
        st.sidebar.write(f"**Task {task.task_code}:** {task.description}")
        task_methods = session.query(Method).filter_by(task_id=task.task_id).all()
        for method in task_methods:
            st.sidebar.write(f"- {method.name}")

if st.sidebar.checkbox("View Methods by Technology"):
    for tech in technologies:
        st.sidebar.write(f"**Technology: {tech.name}**")
        services = session.query(MethodTechnologyService).filter_by(technology_id=tech.technology_id).all()
        for service in services:
            method = session.query(Method).filter_by(method_id=service.method_id).first()
            if method:
                st.sidebar.write(f"- {method.name}")

if st.sidebar.checkbox("View Methods by Type"):
    st.sidebar.header("Methods by Type")
    for method_type, methods_list in methods_by_type.items():
        st.sidebar.subheader(f"Category: {method_type}")
        for method in methods_list:
            st.sidebar.write(f"- {method.name}")

# Main Section: Select Methods to Bundle
st.header("Select Methods to Bundle")
selected_methods = []
method_weights = {}

# Method selection with sliders for weights
for method in all_methods:  # Correct variable
    if st.checkbox(f"{method.name} ({method.maturity})", key=f"method_{method.method_id}"):
        selected_methods.append(method)
        cost_w = st.slider(f"Cost Weight for {method.name}", 0.0, 2.0, 1.0, step=0.1, key=f"cost_{method.method_id}")
        maturity_w = st.slider(f"Maturity Weight for {method.name}", 0.0, 2.0, 1.0, step=0.1, key=f"maturity_{method.method_id}")
        integration_w = st.slider(f"Integration Weight for {method.name}", 0.0, 2.0, 1.0, step=0.1, key=f"integration_{method.method_id}")
        interoperability_w = st.slider(f"Interoperability Weight for {method.name}", 0.0, 2.0, 1.0, step=0.1, key=f"interoperability_{method.method_id}")
        method_weights[method.method_id] = {
            "cost_w": cost_w,
            "maturity_w": maturity_w,
            "integration_w": integration_w,
            "interoperability_w": interoperability_w,
        }

# Monte Carlo simulation
def sample_normal_dist(min_val, max_val, std_dev=0.5):
    """
    Generate a random sample from a normal distribution, clipped to [0, 9].
    """
    mean_val = (min_val + max_val) / 2
    return np.clip(np.random.normal(mean_val, std_dev), 0, 9)

def monte_carlo_simulation(selected_methods, method_weights, n_simulations=1000):
    """
    Perform a Monte Carlo simulation to calculate Integration Readiness Level (IRL).
    """
    irl_scores = []
    for _ in range(n_simulations):
        total_irl = 0
        for method in selected_methods:
            service = session.query(MethodTechnologyService).filter_by(method_id=method.method_id).first()
            if service:
                # Sample each parameter using a normal distribution within the min/max bounds
                cost_score = sample_normal_dist(service.cost_min, service.cost_max)
                maturity_score = sample_normal_dist(service.maturity_min, service.maturity_max)
                integration_score = sample_normal_dist(service.integration_min, service.integration_max)
                interoperability_score = sample_normal_dist(service.interoperability_min, service.interoperability_max)

                # Get weights for each parameter
                weights = method_weights[method.method_id]

                # Calculate a linearly weighted score for each method
                weighted_score = (
                    weights["cost_w"] * cost_score +
                    weights["maturity_w"] * maturity_score +
                    weights["integration_w"] * integration_score +
                    weights["interoperability_w"] * interoperability_score
                )

                # Sum scores across all methods in the bundle (additive aggregation)
                total_irl += weighted_score

        # Normalize by the total weights and number of methods to keep IRL in range
        if selected_methods:
            normalization_factor = sum(weights.values()) * len(selected_methods)
            irl_scores.append(total_irl / normalization_factor)  # Average IRL score
        else:
            irl_scores.append(0)

    return np.array(irl_scores)

# Radar Chart
def plot_radar_chart(method_weights):
    categories = ["Cost", "Maturity", "Integration", "Interoperability"]
    avg_values = [
        np.mean([weights.get(cat.lower() + "_w", 1.0) for weights in method_weights.values()])
        for cat in categories
    ]
    avg_values += avg_values[:1]
    angles = [n / float(len(categories)) * 2 * pi for n in range(len(categories))]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
    ax.fill(angles, avg_values, color="blue", alpha=0.25)
    ax.plot(angles, avg_values, color="blue", linewidth=2)
    ax.set_yticks([])
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories)
    ax.set_title("Radar Chart of Weights", size=16, y=1.1)
    return fig

# Run Simulation
if st.button("Run Simulation") and selected_methods:
    scores = monte_carlo_simulation(selected_methods, method_weights)

    st.write("**Simulation Results:**")
    st.write(f"Mean IRL Score: {np.mean(scores):.2f}")
    st.write(f"Standard Deviation: {np.std(scores):.2f}")
    percentiles = np.percentile(scores, [5, 50, 95])
    st.write(f"5th Percentile: {percentiles[0]:.2f}, Median: {percentiles[1]:.2f}, 95th Percentile: {percentiles[2]:.2f}")

    # Histogram
    st.subheader("IRL Score Distribution")
    fig, ax = plt.subplots()
    ax.hist(scores, bins=50, color='blue', alpha=0.7)
    ax.set_title("Histogram of IRL Scores")
    ax.set_xlabel("IRL Score")
    ax.set_ylabel("Frequency")
    st.pyplot(fig)

    # Radar Chart
    st.subheader("Radar Chart")
    radar_fig = plot_radar_chart(method_weights)
    st.pyplot(radar_fig)
