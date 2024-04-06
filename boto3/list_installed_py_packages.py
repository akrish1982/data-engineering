import subprocess

def get_installed_packages():
  """
  This function uses pip freeze to get a list of installed Python packages and their versions.

  Returns:
      A list of strings, where each string is a package name and version in the format "package_name==version_number".
  """
  # Execute pip freeze command and capture the output
  process = subprocess.Popen(['pip', 'freeze'], stdout=subprocess.PIPE)
  output, error = process.communicate()

  # Decode output from bytes and split into lines
  output_lines = output.decode('utf-8').splitlines()

  # Return the list of installed packages
  return output_lines

# Example usage
installed_packages = get_installed_packages()
for package in installed_packages:
  print(package)