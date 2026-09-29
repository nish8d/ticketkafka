import warnings

import authlib.deprecate  # noqa: F401

# authlib (a dependency of the Schema Registry client) warns about its own httpx integration when
# imported, after forcing its warnings to "always" — so this filter must come after that import.
# It lives here because the package runs before any of its modules import the registry client.
warnings.filterwarnings("ignore", message="The httpx module is deprecated", category=DeprecationWarning)
