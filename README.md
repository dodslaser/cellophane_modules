# Cellophane Modules

<p>
<a href="https://github.com/ClinicalGenomicsGBG/cellophane_modules/actions/workflows/push-main.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/ClinicalGenomicsGBG/cellophane_modules/push-main.yml?label=CI"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

This is the main repo containing all public cellophane modules. Modules aim to be as generalized as possible for maximum reusability.

## Using a module

Modules can be added, updated, or removed from a wrapper using the cellophane CLI.

```shell
# Add
python -m cellophane add my_module

# Update
python -m cellophane update my_module

# Remove
python -m cellophane rm my_module
```

Hooks, Mixins and Runners are automatically detected by cellophane on runtime. Configuration schemas will be automatically merged, and configuration options made available.

## Adding a new module to the repo

To add a module to this repo, a minimal definition needs to be added to `modules.json`. The definition will be automatically updated by the CI/CD pipeline.

```json
{
  // ...

  "my_module": {
    "path": "modules/my_module/",
    "latest": "dev",
    "versions": {
      "dev": {
        "tag": "main",
        "cellophane": [">0.0.0"]
      }
    }
  }

  // ...
}
```