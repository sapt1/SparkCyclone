package com.nec.ve.ncc

import org.scalatest.freespec.AnyFreeSpec

/**
 * We have to consider the following use-cases:
 * - Running production-like deployments
 * - Development level testing:
 *   - Where we are actively refining the core library, and would like to recompile on every go
 *   - Where we are not refining the core library, and would like to avoid recompilations
 *   - When making development-level changes, we don't want to deal with cached libraries by error
 *
 * To meet these constraints, this is the approach going forward:
 * - During deployment time, we pre-compile the library fully and package it with the JAR.
 * - During development time (VE-only, not CMake)
 *   1. We `rsync` all the files into a `target` directory.
 *   2. Use `make` to check if anything needs to be recompiled, and produce a `.so`.
 *   3. We link to this `.so` at compile-time from the main library.
 *   4. All the headers still come from `src/main/resources`.
 *
 * Deployment-time we can reuse the same mechanism as part of `assembly`
 */
final class FastVeKernelCompilerSpec extends AnyFreeSpec {
  info("The key idea with this is to create a minimalistic Makefile for the library in the user's context")
  info("And then depend on that as the pre-built library")
}
