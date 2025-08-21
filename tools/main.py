import subprocess
import sys

def run_script(script_name):
    print(f"➡️ Running {script_name} ...")
    try:
        subprocess.run([sys.executable, script_name], check=True)
        print(f"✅ {script_name} finished successfully\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("🚀 Starting full pipeline")

    run_script("generate_students.py")
    run_script("generate_jobs.py")
    run_script("ingestion_microservice/ingestJob.py")

    print("🎉 Pipeline finished!")
