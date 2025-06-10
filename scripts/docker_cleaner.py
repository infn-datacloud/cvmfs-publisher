import subprocess

def get_docker_images():
    """Return a list of (image_id, tag) tuples."""
    result = subprocess.run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}"],
                            capture_output=True, text=True)
    lines = result.stdout.strip().split("\n")
    images = []
    for line in lines:
        if line:
            full_tag, image_id = line.split()
            repo, tag = full_tag.rsplit(":", 1)
            images.append((image_id, tag))
    return images

def delete_non_latest_images(images):
    deleted = 0
    for image_id, tag in images:
        if tag != "latest":
            print(f"Deleting image {image_id} (tag: {tag})")
            subprocess.run(["docker", "rmi", "-f", image_id])
            deleted += 1
    print(f"\n✅ Deleted {deleted} image(s) (non-latest).")

def main():
    images = get_docker_images()
    delete_non_latest_images(images)

if __name__ == "__main__":
    main()

