package com.yutuer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yutuer.database.WikiDatabaseVerticle;

public class MainVerticle extends AbstractVerticle
{
	private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

	@Override
	public void start(Future<Void> startFuture) throws Exception
	{
		Future<String> dbVerticleDeployment = Future.future();
		vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment.completer());

		dbVerticleDeployment.compose(
				id ->
				{
					Future<String> httpVerticleDeployment = Future.future();
					vertx.deployVerticle("com.yutuer.HttpServerVerticle", new DeploymentOptions().setInstances(2),
							httpVerticleDeployment.completer());
					return httpVerticleDeployment;
				}).setHandler(ar ->
		{
			if (ar.succeeded())
			{
				startFuture.complete();
			}
			else
			{
				startFuture.fail(ar.cause());
			}
		});
	}

	public static void main(String[] args)
	{
		Vertx vertx = Vertx.vertx();

		vertx.deployVerticle(new MainVerticle());

	}
}
