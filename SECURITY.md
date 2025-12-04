# Security Notice

## Pre-Release Checklist

Before making this repository public, the following security steps **MUST** be completed:

### ⚠️ Critical: Git History Contains Sensitive Data

The git history of this repository contained sensitive information that has been identified and must be removed before publishing. A fresh repository should be created with a clean history.

**Sensitive items identified in history:**

1. **SSH Keys** - Private and public keys were committed
2. **API Keys** - Prefect Cloud API keys
3. **Database Credentials** - ClickHouse passwords
4. **Infrastructure Details** - IP addresses and instance identifiers

### Recommended Approach: Squash to Single Commit

The safest approach for open-sourcing is to:

1. Create a new repository
2. Copy the cleaned files (without `.git` directory)
3. Make a single initial commit
4. This eliminates all historical sensitive data

```bash
# Create a clean copy
mkdir dataos-pipeline-clean
cp -r dataos-pipeline/* dataos-pipeline-clean/
rm -rf dataos-pipeline-clean/.git

# Initialize fresh repository
cd dataos-pipeline-clean
git init
git add .
git commit -m "Initial release - DataOS Pipeline (archived)"
```

### Alternative: BFG Repo-Cleaner

If preserving history is required, use [BFG Repo-Cleaner](https://rtyley.github.io/bfg-repo-cleaner/):

```bash
# Install BFG
brew install bfg

# Remove sensitive files from history
bfg --delete-files privkey
bfg --delete-files pubkey
bfg --delete-files .env

# Remove specific patterns from all commits
bfg --replace-text patterns.txt  # File containing patterns to replace

# Clean up
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

### Post-Cleanup Verification

After cleanup, verify no sensitive data remains:

```bash
# Search for API key patterns
git log -p --all | grep -E "(pnu_|ALAb|734986ce)" | head -20

# Search for IP addresses
git log -p --all | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | head -20

# Search for key files
git log --all --full-history -- "*.pem" "*.key" "privkey" "pubkey"
```

### Credential Rotation

**Important:** All credentials found in the history should be rotated:

- [ ] Rotate Prefect API keys
- [ ] Change ClickHouse passwords
- [ ] Regenerate SSH keys
- [ ] Update any related infrastructure credentials

---

## Security Best Practices

For future development:

1. **Never commit secrets** - Use environment variables
2. **Use `.gitignore`** - Prevent accidental commits
3. **Pre-commit hooks** - Scan for secrets before commit
4. **Secret scanning** - Enable GitHub secret scanning on the repository

## Contact

For security concerns, please contact the repository maintainers.

