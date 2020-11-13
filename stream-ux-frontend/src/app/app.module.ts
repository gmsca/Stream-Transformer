import { HttpClientModule } from '@angular/common/http';
import { NgModule, CUSTOM_ELEMENTS_SCHEMA, Injector } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { BrowserModule } from '@angular/platform-browser';
import { InjectableRxStompConfig, RxStompService, rxStompServiceFactory } from '@stomp/ng2-stompjs';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { MessageStreamComponent } from './message-stream/message-stream.component';
import { myRxStompConfig } from './rx-stomp.config';
import { HeaderComponent } from './header/header.component';
import { BreadcrumbsComponent } from './breadcrumbs/breadcrumbs.component';
import { environment } from 'src/environments/environment';

@NgModule({
   declarations: [
      AppComponent,
      MessageStreamComponent,
      HeaderComponent,
      BreadcrumbsComponent
   ],
   imports: [
      BrowserModule,
      HttpClientModule,
      FormsModule,
      ReactiveFormsModule,
      AppRoutingModule
   ],
   providers: [
      {
         provide: InjectableRxStompConfig,
         useValue: myRxStompConfig
      },
      {
         provide: RxStompService,
         useFactory: rxStompServiceFactory,
         deps: [InjectableRxStompConfig]
      }
   ],
   schemas: [CUSTOM_ELEMENTS_SCHEMA],
   bootstrap: [
      AppComponent
   ]
})
export class AppModule {
   constructor(private injector: Injector) {
      let gmsWcScript = document.createElement("script");
      gmsWcScript.type = "module";
      gmsWcScript.src = environment.gmsWebComponents;
      document.body.appendChild(gmsWcScript);
   }
}
