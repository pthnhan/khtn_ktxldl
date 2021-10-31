import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="youtube_tools",
    version="0.0.2",
    author="Nhan Thanh",
    author_email="nhanmath97@gmail.com",
    description="Pull data trending from youtube, and somethings",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["youtube_tools"] + setuptools.find_packages(include=["youtube_tools.*", "youtube_tools"]),
    package_dir={"youtube_tools": "youtube_tools"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)